#!/bin/bash -u
# No point in setting in shebang as bash typically overrides this ; redundant for script but critical for testing in interactive shell
set +m
# Care and feeding of mapfile
shopt -s lastpipe || (echo "ERROR: 'lastpipe' shell option not supported by shell. Exiting." 1>&2 ; exit 1)

# FIXME check ALL return values
# TODO confirm that boolean queries all work correctly!
# TODO traps!
# TODO consider adding something to explicitly run for one dataset name i.e. for running initial in parallel

#FIXME move this to /etc
config=/root/backup/pcic_backup.conf

if [[ -f "${config}" ]] ; then
    . "${config}"
else
    echo "CRITICAL: Config file '${config}' not found!  Exiting." 1>&2
    exit 1
fi

rsyncopts=(--delete --delete-excluded --archive --one-file-system --hard-links --sparse --numeric-ids --rsync-path="/usr/bin/ionice -c 3 /usr/bin/rsync" --stats -hh --protect-args)
timestampformat='+%Y-%m-%dT%H:%M%Z'
localtz="TZ=\"America/Vancouver\""
sqlite=/usr/bin/sqlite3

if [[ ! -z "${exclusion_file}" ]] ; then
    rsyncopts+=(--exclude-from="${exclusion_file}")
fi




###########################
###                     ###
###  MAIN LOOP TURN ON  ###
###                     ###
###########################
"${sqlite}" "${db}" "SELECT name FROM $table WHERE enable == 'enabled'" | mapfile -t datasets
for name in "${DATASETS[@]}" ; do
    ## FIXME should UNSET variables @ end of each loop iteration, or use a subshell

    ###############
    ### Trylock ### (or increment failure count)
    ###############
    "${sqlite}" "${db}" <<EOF
        BEGIN;
        UPDATE $table SET lock_pid = $$, failed_trylocks = 0 WHERE name == '$name' AND lock_pid IS NULL;
        UPDATE $table SET failed_trylocks = (failed_trylocks + 1) WHERE name == '$name' AND lock_pid IS NOT NULL;
        COMMIT;
EOF
    # Check if trylock failed and handle failure
    if [[ "$("${sqlite}" "${db}" "SELECT lock_pid FROM $table WHERE name == '$name'")" != $$ ]] ; then
        echo "WARNING: Failed to acquire lock for dataset '$name'" 1>&2
        continue
    fi

    ##########################################################################
    ### Sanity-check all 4 timestamps in DB, else set error state and bail ###
    ##########################################################################
    # (aborted backup is not handled here as that is not an error)
    # NB: This does ignore last_* being _after_ next/previous. Not intending to change this at present.
    # MPN has a spreadsheet of the various combinations that result in this check.  Ask if I haven't remembered to leave a copy around somewhere.
    # TODO check return value (move result into a var)
    # TODO consider making this more informative as to what problem was detected.
    # TODO desirable to confirm previous/next (if set) are BEFORE NOW, but difficult in SQLite
    # TODO this should also check for correctly-formatted previous/next
    if (( $("${sqlite}" "${db}" "SELECT ((last_finish IS NOT NULL AND (last_start IS NULL OR previous_backup IS NULL or next_backup IS NULL)) OR (last_start IS NOT NULL AND next_backup IS NULL) OR (previous_backup IS NOT NULL AND next_backup IS NOT NULL AND previous_backup >= next_backup)) FROM $table WHERE name == '$name'") )) ; then
        "${sqlite}" "${db}" "UPDATE $table SET enable = 'error', last_error = inconsistent database timestamps for this dataset', lock_pid = NULL WHERE name == '$name'"
        echo "ERROR: inconsistent database timestamps for dataset '$name'" 1>&2
        break
    fi

    ########################################################
    ### Get all relevant database vars before proceeding ###
    ########################################################
    remote_host="$("${sqlite}" "${db}" "SELECT connect_as FROM ${table} NATURAL JOIN host_mappings WHERE name = '${name}'")"
    zpool="$("${sqlite}" "${db}" "SELECT zpool FROM ${table} WHERE name = '${name}'")"
    previous_backup="$("${sqlite}" "${db}" "SELECT previous_backup FROM ${table} WHERE name = '${name}'")"
    next_backup="$("${sqlite}" "${db}" "SELECT next_backup FROM ${table} WHERE name = '${name}'")"


    ### FIXME need to confirm here that (if next_backup is set) next_backup is at least a little (2h?) old, otherwise don't try to proceed!


    ########################################################
    ### Confirm presence of mounted source+dest datasets ### 
    ########################################################
    # Check source (FIXME should also check for status 255)
    # FIXME also check that mountpoint is in fact $remote_root/$name
    ssh "${remote_host}" "zfs list ${zpool}/${name} &>/dev/null && mountpoint \$(zfs get -H -o value mountpoint ${zpool}/${name})" \
        || (echo "ERROR: remote source dataset '${zpool}/${name}' does not exist" 1>&2 ; exit 1)
    # Check dest FIXME make this exit graceful
    mountpoint "${backup_root}/${name}" || (echo "ERROR: local path '${backup_root}/${name}' does not exist" 1>&2 ; exit 1)


    ## If next is not set, figure it out.
    if [[ -z "${next_backup}" ]] ; then
        ## Decide whether we're working from previous or from snapshots for 'next'
        if [[ -z "${previous_backup}" ]] ; then
            ## Infer oldest available from source snapshots
            # REQUIRES shopt 'lastpipe'
            ssh "${remote_host}" "zfs list -H -o name -t snapshot -d 1 ${zpool}/${name}" \
                | sed -r "s:^${zpool}/${name}@::" \
                | grep -E '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}UTC$' \
                | sort \
                | mapfile -t snapstamps
            # Check PIPESTATUS[0] here to see how `ssh` fared
            # FIXME check exit status ^^
            # Select the first midnight one
            for snapstamp in "${snapstamps[@]}" ; do
                localstamp=$(date -d $snapstamp +%Y-%m-%dT%H:%M)
                [[ "$localstamp" =~ 00:00$ ]] || continue
                next_backup="$snapstamp"
                # Clearly we succeeded, so...
                break
            done
            # Check that we found one, otherwise ERROR cannot find or does not exist
            if [[ -z "$next_backup" ]] ; then
                # FIXME spew error but do not set error state here as this is transient (leave relevant part of this comment)
                # Bail from this dataset
                continue
            fi
            # TODO consider checking that timestamp VALUE actually makes sense (i.e. is before now)
        else
            ## Add increment to previous_backup
            # Round to nearest midnight and ignore +/-1 hour DST change as part of that
            next_backup_localtime="$(date -d "${previous_backup} 1 day 12 hours" +%Y-%m-%dT00:00%Z)"
            next_backup="$(date -u -d "${next_backup_localtime}" "${timestampformat}")"
            unset next_backup_localtime
        fi
    fi

    ############################################################
    ### Detect (using DB) and handle aborted previous backup ###
    ############################################################
    #
    # This could potentially be improved by having DIFFERENT names when `cp`
    # is incomplete vs when `rsync` is incomplete as rsync can be resumed.
    #
    aborted="$("${sqlite}" "${db}" "SELECT (last_start IS NOT NULL AND (last_finish IS NULL OR (last_start > last_finish))) FROM $table WHERE name == '$name'")"
    if (( aborted )) ; then
        # See if it actually completed but wasn't marked as complete;
        # if so, update and continue to do next backup.
        if [[ -d "${backup_root}/${name}/${next_backup}" ]] ; then
            # Correct database and change local vars accordingly
            previous_backup="${next_backup}"
            next_backup_localtime="$(date -d "${previous_backup} 1 day 12 hours" +%Y-%m-%dT00:00%Z)"
            next_backup="$(date -u -d "${next_backup_localtime}" "${timestampformat}")"
            unset next_backup_localtime
            "${sqlite}" "${db}" <<EOF
                BEGIN;
                UPDATE ${table} SET previous_backup = '${previous_backup}', next_backup = '${next_backup}' WHERE name = '${name}';
                UPDATE ${table} SET first_backup = '${previous_backup}' WHERE name = '${name}' AND first_backup IS NULL;
                COMMIT;
EOF
        elif [[ -d "${backup_root}/${name}/${next_backup}.incomplete" ]] ; then
            rm -Rf "${backup_root}/${name}/${next_backup}.incomplete"
        fi
    fi

    ### (FIXME) Confirm that source snapshot exists and is mounted, and that dest and dest.incomplete do not exist ###
    # set ERROR state if dest exits, otherwise just(?) log ERROR if source does not exist
    # FIXME code here


    ## FIXME THIS IS NOT THE COMPLETE, ROBUST THING TO BE DOING HERE TO ENSURE THAT THE REMOTE SNAPSHOT IS MOUNTED.  THIS IS JUST HERE SO I CAN TEST THE REST.  NEED TO INVESTIGATE FURTHER WHAT THE "IDEAL" APPROACH TO THIS IS.
    ssh "${remote_host}" "ls \"${remote_root}/${name}/.zfs/snapshot/${next_backup}\"" &>/dev/null

    ##################################
    ### Let DB know we're starting ###
    ##################################
    echo "INFO: Setting next_backup in database and commencing backup @ '${next_backup}' for '${name}'" 1>&2
    "${sqlite}" "${db}" "UPDATE ${table} SET next_backup = '${next_backup}', last_start = '$(date +%s)' WHERE name = '${name}'"

    ################################################################
    ### Set up (dupe) hardlinks if there is a 'previous_backup' ####
    ################################################################
    if [[ ! -z "${previous_backup}" ]] ; then
        cp -al "${backup_root}/${name}/${previous_backup}" "${backup_root}/${name}/${next_backup}.incomplete"
        case $? in
            0)
                
                ;;
            *)
                echo "ERROR: Unknown error renaming from .incomplete for '${name}' @ '${next_backup}'." 1>&2
                ;;
        esac
        echo "INFO: Finished cp for '${name}' @ '${next_backup}', now updating database..." 1>&2
    fi


    ####################################################################################
    ### Leverage 'R' deltas from `zfs diff` here once diff actually works correctly. ###
    ####################################################################################



    #############
    ### rsync ###
    #############
    /usr/bin/rsync "${rsyncopts[@]}" "${remote_host}:${remote_root}/${name}/.zfs/snapshot/${next_backup}/" "${backup_root}/${name}/${next_backup}.incomplete/"
    
    case $? in
        0) 
            ## A-OK
            ;;
        ## TODO handle some known-cases explicitly here like syntax errors (which should not happen but might as well add them) etc.
        *)
            echo "ERROR: Unknown error running rsync for '${name}' version '${next_backup}'." 1>&2
            continue
            ;;
    esac
    echo "INFO: Finished rsync for '${name}' @ '${next_backup}', now renaming..." 1>&2

    ### TODO sanity-check that resulting backup seems to contain enough files or at least sentinel file, else ERROR
    
    ### We're done, so rename it
    mv "${backup_root}/${name}/${next_backup}.incomplete" "${backup_root}/${name}/${next_backup}"
    case $? in
        0)
            
            ;;
        *)
            echo "ERROR: Unknown error renaming from .incomplete for '${name}' @ '${next_backup}'." 1>&2
            ;;
    esac

    ###############################################################
    ### Release lock (and update other things at the same time) ###
    ###############################################################
    # update last_finish and previous_backup and next_backup in DB now that we've been successful ###
    previous_backup="$next_backup"
    next_backup_localtime="$(date -d "${previous_backup} 1 day 12 hours" +%Y-%m-%dT00:00%Z)"
    next_backup="$(date -u -d "${next_backup_localtime}" "${timestampformat}")"
    unset next_backup_localtime
    # Update DB and release lock
    "${sqlite}" "${db}" <<EOF
        BEGIN;
        UPDATE ${table} SET last_finish = '$(date +%s)', previous_backup = '${previous_backup}', next_backup = '${next_backup}', lock_pid = NULL WHERE name = '${name}';
        UPDATE ${table} SET first_backup = '${previous_backup}' WHERE name = '${name}' AND first_backup IS NULL;
        COMMIT;
EOF

    ####################
    ### Housekeeping ###
    ####################
    ## TODO delete old backups and old snapshots according to retention policy (not yet defined)

done