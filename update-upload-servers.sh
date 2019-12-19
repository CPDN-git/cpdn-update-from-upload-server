#!/bin/sh
#
# Script Name: update_jasmin-upload
#
# Author:  Peter Chiu
# Purpose: update jasmin-upload data for Sarah Sparrow
#
# Usage:   simple (# sh /csupport/script/update_jasmin-upload 
# History: created on: 10 Sep 2014 
#          changed on: 21 Aug 2015 - added in wah
#                      30 Sep 2015 - migrated to JASMIN2
#+++
# Define work path.
#---
PATH=$PATH:/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin:~/.local/bin
# 
# Define data movement function
# $1 = $upload_endpoint
# $2 = $jasmin_endpoint
# $3 = location
# $4 = $p
# $5 = $b
# $6 = $log

cd /home/wallom/cpdn-update-from-upload-server

move_data() {
	local upload_endpoint=$1
	local jasmin_endpoint=$2
	local location=$3
	local p=$4
	local b=$5
	local log=$6
	echo "globus transfer $upload_endpoint:/storage/boinc/project_results/$p/$b/successful/ $jasmin_endpoint:$location/$p/$b/successful/ -r --preserve-mtime -s checksum --jmespath 'task_id' --format=UNIX --notify failed,inactive -v" >> ${log}
        task_id="$(globus transfer $upload_endpoint:/storage/boinc/project_results/$p/$b/successful/ $jasmin_endpoint:$location/$p/$b/successful/ -r --preserve-mtime -s checksum --jmespath 'task_id' --format=UNIX --notify failed,inactive -v)"
        echo "Waiting on TRANSFER TASK' '$task_id'" >> ${log}
        globus task wait "$task_id" --polling-interval 60
        if [ $? -eq 0 ]; then
        	echo "TRANSFER TASK $task_id completed successfully" >> ${log}

                # Now get list of successfully transfered files in this transfer
		globus task show -tF json $task_id |grep source > ${task_id}.files
		# Now check to make sure ${task_id}.files 
		if [ -s ${task_id}.files ]
		then
			# Now process $task_id.files to only get source filename
			while read file; do
				file=${file/\"source_path\"\: \"/}
				file=${file/\"/}
				file=${file/\/\//\/}
				echo $file >> ${task_id}.files.temp
			done < ${task_id}.files
			mv -f ${task_id}.files.temp ${task_id}.files
			# run the delete for the specific $task_id.files file
			# Need to check whether this file is of non-zero size
			delete_id="$(cat ${task_id}.files | globus delete --jmespath 'task_id' --format=UNIX --notify failed,inactive --batch $upload_endpoint)"
			echo "Waiting on DELETE TASK $delete_id" >> ${log}
			globus task wait "$delete_id" --polling-interval 60
			if [ $? -eq 0 ]; then
                		echo "DELETE TASK $delete_id completed successfully" >> ${log}
				rm ${task_id}.files
			else
				echo "DELETE TASK $delete_id failed!" >> ${log}
			fi
		else
			echo "TRANSFER TASK $task_id no files required transfer" >> ${log}
			rm ${task_id}.files
		fi
        else
                echo "TRANSFER TASK $task_id failed!" >> ${log}
        fi
}

#+++
# Check if this job is already running
#---
mailing=david.wallom@oerc.ox.ac.uk
job_lock=${0}.running
if [ -f ${job_lock} ]
then
  sleep `expr $RANDOM % 100`
  if [ -f ${job_lock} ]
  then
    mail -s "$0.running - current job still running? please check" \
    ${mailing} < /etc/motd
    exit
  fi
fi
touch ${job_lock}
#+++
# Save current log
#---
log=$0.log
if [ -f ${log} ]
then
  date >> ${log}.saved 
  cat ${log} >> ${log}.saved
  cat /dev/null > ${log}
fi
#
# Configure Globus transfer
#
# Setup remote endpoint location
jasmin_endpoint=4cc8c764-0bc1-11e6-a740-22000bf2d559

#
# Determine which upload server this is running on
upload=`uname -n`
if [ "$upload" == "jasmin-upload.cpdn.org" ]
  then
     echo $upload >> ${log} 
     upload_endpoint=671a1294-9d86-11e9-a378-0a2653bc2660
elif [ "$upload" == "upload3.cpdn.org" ] 
  then
     echo $upload >> ${log}
     upload_endpoint=671a1294-9d86-11e9-a378-0a2653bc2660    
else
     echo "Can find an endpoint!" >> ${log}
     upload_endpoint=671a1294-9d86-11e9-a378-0a2653bc2660                                
fi
echo "Upload Endpoint is $upload_endpoint" >> ${log}
# 
# Test endpoint locations
#
globus endpoint show $jasmin_endpoint  >> ${log}
globus endpoint show $upload_endpoint  >> ${log}
globus endpoint activate --force --myproxy -U wallom -P UYW06s8ext $jasmin_endpoint >> ${log}
#echo "*** JASMIN GWS ***"
#globus ls $jasmin_endpoint:/gws/nopw/j04/cpdn_rapidwatch
echo "${0} - start rsync on `date`..." >> ${log}
echo "*** JASMIN-UPLOAD ***"  >> ${log}
#set counter of globus jobs to 0
# Need to work out the list of batches per project in the jasmin-upload service
while read p; do
	# Get list of active projects
	echo "*** Project = $p ***"  >> ${log}
    	globus ls $upload_endpoint:/storage/boinc/project_results/$p >$p.batch.list
	#Now read batch list and get list of batches content in each project
    	while read b; do   
		# Check that the project/batch/successful directory actually has WU in it
        	if [ -d /storage/boinc/project_results/$p/$b/successful ] 
		   then
			#echo "/storage/boinc/project_results/$p/$b/successful exists."
            		count=`ls /storage/boinc/project_results/${p}/${b}/successful |wc -l`
			#echo $count 
            		if [ "$count" -gt "0" ] 
			   then
                		# Globus transfer from project $p results in batch $b in jasmin-upload to GWS
                		# Test which project we are transfering since some have different GWS requirements
#				if [ "$p" == "aflame" ]
#				   then
#		    			location=/gws/nopw/j04/mohc_shared/users/ssparrow/project_results
#                              elif [ "$p" == "lotus" ]
                               if [ "$p" == "lotus" ]
                                  then
                                       location=/gws/nopw/j04/gotham/gotham/cpdn_data
				elif [ "$p" == "gotham" ] 
				   then
		    			location=/gws/nopw/j04/gotham/gotham/cpdn_data
				elif [ "$p" == "docile" ] 
				   then
		    			location=/gws/nopw/j04/docile/project_results
	        		else
		    			location=/gws/nopw/j04/cpdn_rapidwatch/project_results
				fi
				# Call data movement function
				echo "move_data $upload_endpoint $jasmin_endpoint $location $p $b $log" >> ${log}				
				move_data $upload_endpoint $jasmin_endpoint $location $p $b $log &
            		fi
        	fi
    	done < $p.batch.list
done < projects.txt

wait

echo "OUT OF THE LOOP :)" >> ${log}
#
  status=${?}
  echo "${0} - ended with status $status on `date`." >>${log}
  echo >> ${log}
  if [ ${status} -gt 0 ]
  then
    mail -s "$0 - rsync issue detected on `hostname`" ${mailing} < $log
  fi
if [ -f ${job_lock} ]
then
  rm -rf *.batch.list
  rm -rf ${job_lock}
fi
