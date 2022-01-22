#!/bin/bash
# Created By Akshay Pradeep
#

source ~/.bashrc
source hbase_tunne.cfg
>/tmp/merge_out

BUSSY=0

export PS4='+($(basename ${BASH_SOURCE[0]}):${LINENO}): ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -x


printlog(){
        echo "`date +'%d/%m/%Y %H:%M:%S'` | $1" >> tunne.log
}

check_mc(){  #ONLY FOR TESTING -TEST-
        val=`cat mc|wc -l`
		printlog "[INFO] checking MC"
        return $val
}

find_master(){
	printlog "[INFO] finding the active HMaster"
	hbase shell << HERE > /tmp/hbase_detailed_status.txt
	status 'detailed'
HERE
	hmaster_ip=`grep 'active master' /tmp/hbase_detailed_status.txt | cut -d':' -f2 | tr -d ' '`
	printlog "[INFO] HMaster running in $hmaster_ip"
}


check_major_compacting(){
        >/tmp/check_major_compacting_ap.tmp
        curl -s "http://${hmaster_ip}:${hmaster_port}/master-status" |grep 'href=table.jsp?name='|cut -d'=' -f3|cut -d'>' -f1  > /tmp/all_tables_ist.tmp
	cat $tunning_list|cut -d, -f1 |while read table_master
	do
		cat /tmp/all_tables_ist.tmp |grep $table_master |while read table
		do
                	curl -s "http://${hmaster_ip}:${hmaster_port}/table.jsp?name=${table}" |grep '<td>Enabled</td>' -A10|tr -d '\n'| awk -F '<td>|</td>' -v table=$table '{if($4=="true")print table" : " $10}' >> /tmp/check_major_compacting_ap.tmp
		done
	done
    	count_major=`grep -ic major /tmp/check_major_compacting_ap.tmp`
	return $count_major
}


check_split_merge(){

	check_split_merge_count=$(curl -s 'http://ctvl-icms-rst13:16010/master-status' |awk '/<th>Namespace<\/th>/,/<\/table>/' |sed 's/ //g;s/<\/th>/,/g;s/<\/td>/,/g;s/<th>//g;s/<td>//g' |sed 's/<\/a>//g;s/<tr>//g'|sed -e 's/<ahref=table.jsp?name=.*>//g;' |tr -d '\n' | sed 's/<\/tr>/\n/g' | grep $1 |awk -F',' '{if ( $6!=0||$7!=0) print $2","$6","$7}'|wc -l)
	return $check_split_merge_count
}


get_all_tables(){
	printlog "[INFO] collecting list of tables in the HBase"
	curl -s "http://${hmaster_ip}:${hmaster_port}/master-status" |grep 'href=table.jsp?name='|cut -d'=' -f3|cut -d'>' -f1  > /tmp/all_tables_ist.tmp
}

split_tunne_list(){
	printlog "[INFO] splting commands for table $1 , $2 commands each in a set"
	split -l $2 "${tunne_list}/$1" "${split_list}/${1}.merge_region_file."
}

clear_split_list(){
	if [ -z $split_list  ]
        then
                printlog "[ERROR] split dir is null"
        fi
        rm -f $split_list/*merge_region_file*
}

clear_tunne_list(){
	if [ -z $tunne_list  ]
        then
                printlog "[ERROR] tunne dir is null"
        fi
	rm -f $tunne_list/merge-*-tunne_list.lst
	printlog "[INFO] tunne list cleared"

}

find_merge(){
    table=$1
	min_size=`echo $2 | numfmt --from=iec`
	max_size=`echo $3 | numfmt --from=iec`
	#finding the region size for a particular table using hdfs command 
	printlog "[INFO] finding mergeble regions for $table | min size: `echo $min_size|numfmt --to=iec` | max size: `echo $max_size|numfmt --to=iec` "
	printlog "[INFO] extracting region sizes for $table"
	hdfs dfs -du ${hbase_root_dir}/${table} | sort -n -k 1|grep -Ev 'tmp|tabledesc' > /tmp/find_region_size.out
	printlog "[INFO] extracting meta data for $table"
	hbase shell <<EOF > /tmp/find_table_meta.out
       		 scan 'hbase:meta', {FILTER=>"PrefixFilter('$table')", COLUMNS=>['info:regioninfo']}
EOF

        cat /tmp/find_table_meta.out|grep $table |grep -v "scan 'hbase:meta'" > /tmp/find_table_meta.out_tmp
        mv /tmp/find_table_meta.out_tmp /tmp/find_table_meta.out

		#from the hdfs output filter out the regions to perfom merge 
        cat /tmp/find_region_size.out|awk -v max=$min_size '{if($1<max)print $2}'|awk -F '/' '{print $NF}'|while read region
        do
		is_first=0
		is_last=0
		is_skip=0
		#printlog "[INFO] checking region $region"
                region_size=`grep $region /tmp/find_region_size.out |awk '{print $1}'`
                pre_region=`grep ${region} /tmp/find_table_meta.out -B1|cut -d. -f2|head -1`
                post_region=`grep ${region} /tmp/find_table_meta.out -A1|cut -d. -f2|tail -1`
                pre_region_size=`grep $pre_region /tmp/find_region_size.out |awk '{print $1}'`
                post_region_size=`grep $post_region /tmp/find_region_size.out |awk '{print $1}'`
		
                if [ $region = $post_region ]
                then
                       is_last=1
                       is_first=0
                fi
                if [ $region = $pre_region ]
                then
                       is_last=0
                       is_first=1
                fi

        		if [ \( $post_region_size -le $max_size \) -a \( $is_last -ne 1 \) ]
				then
        	    	merge_region=$post_region
					merge_region_size=$post_region_size
				else
					if [ \( $pre_region_size -le $max_size \) -a \( $is_first -ne 1 \) ] 
						then	
							merge_region=$pre_region
							merge_region_size=$pre_region_size
						else
							is_skip=1
					fi
							
        		fi
	
	        	grep -E "$region|$merge_region" $tunne_list/merge-${table}-tunne_list.lst
	        	if [ \( $? -ne 0 \) -a \( $is_skip -ne 1 \) ]
	        	then
					printlog "[INFO] adding $table | `echo $region_size|numfmt --to=iec` | merge_region '$region','$merge_region'| `echo $merge_region_size |numfmt --to=iec`"
					#echo " $table | `echo $region_size|numfmt --to=iec` | merge_region '$region','$merge_region'| `echo $merge_region_size |numfmt --to=iec`" >> /tmp/merge_out
					echo "merge_region '$region','$merge_region'" >> $tunne_list/merge-${table}-tunne_list.lst
	        	else
					#printlog "skipping $table | `echo $region_size|numfmt --to=iec` | merge_region '$region','$merge_region'| `echo $merge_region_size |numfmt --to=iec` < `echo $max_size |numfmt --to=iec`|$is_first |$is_last|$is_skip"
					printlog "[INFO] skipping $table | `echo $region_size|numfmt --to=iec` $region' |pre region size : `echo $pre_region_size|numfmt --to=iec` | post region size : `echo $post_region_size|numfmt --to=iec` |is first: $is_first |is last: $is_last|is skip: $is_skip"
	        	fi
        	    unset pre_region post_region pre_region_size post_region_size
        done
}


tick_tick(){

	if [ \( "$script_start_time" -le `date  +%H%M`  \) -a \( "$script_end_time" -le `date  +%H%M`  \) ]
	then 
		printlog "[INFO] Working Hours are over exiting script"
		exit
	fi

}

exc_hbase_shell() {

        hbase shell <<EOF
	`cat ${1}`

EOF
}

do_major_compact() {

	printlog "[INFO] major_compacting '$split_table'"
        hbase shell <<EOF
	`echo "major_compact '$split_table'"`

EOF


}


#########################################################################################################################################


printlog "[INFO] Initiating HBASE tunning - Merge Region & Major Compacting"
tick_tick
clear_tunne_list
find_master
get_all_tables
cat $tunning_list |grep -v '#'|while read list
do
	grep `echo $list |cut -d, -f1` /tmp/all_tables_ist.tmp |while read table_find
	do	
		find_merge $table_find `echo $list |cut -d, -f2` `echo $list |cut -d, -f3`
	done
done
clear
#cat $tunne_list/merge-*
clear_split_list
cat $tunning_list |while read list
do
	split_count=`echo $list|cut -d, -f4`
	ls $tunne_list |grep `echo $list|cut -d, -f1` |while read tunne_file
	do
		split_tunne_list "$tunne_file" "$split_count" 
	done
done





cat $tunning_list |while read list
do
	ls $split_list|grep `echo $list |cut -d, -f1` |cut -d'-' -f2|sort |uniq |while read split_table
	do
		printlog "[INFO] Initiating spling for table $split_table "
		ls $split_list | grep $split_table |while read command_file
		do
			tick_tick
			check_split_merge $split_table
			if [ $? -ne 0  ]
                        then
                                BUSSY=1
                                printlog "[INFO] MERGING IS RUNNING ALLREADY WAITING FOR FINISH"
				sleep $merge_check_intervel
				check_split_merge $split_table
				while [ $BUSSY -eq 1 ]
				do
					sleep $merge_check_intervel
					tick_tick
					check_split_merge $split_table
					if [ $? -eq 0 ]
					then
						BUSSY=0
						printlog "[INFO] MERGING FINISHED"
					fi
				done
                        fi

			#check_major_compacting
			check_major_compacting
			if [ $? -ge $max_mc  ]
			then
        			BUSSY=1
				printlog "[INFO] MAJOR COMPACTION IS RUNNING ALLREADY WAITING FOR FINISH"
			fi
			while [ $BUSSY -eq 1 ]
			do
        			sleep $mc_check_intervel
				tick_tick
        			#check_major_compacting
        			check_major_compacting
        			if [ $? -lt $max_mc ]
       				then
                			BUSSY=0
					printlog "[INFO] MAJOR COMPACTION FINISHED CONTINE NEXT SET"
        			fi
			done

			printlog "[INFO] Exicuting command set $command_file"
			cat $split_list/$command_file
			exc_hbase_shell "$split_list/$command_file"
			sleep $mc_delay
			do_major_compact

		done
	done
done

