#!/bin/bash

max_size=2

working_dir='/opt/akshay/etc/script/script_temp/'
>$working_dir/merge_out

printlog(){

	echo "`date +'%d/%m/%Y %H:%M:%S'` | $1"
}

find_table_meta(){
	table=$1
	hbase shell <<EOF > $working_dir/find_table_meta.out
	scan 'hbase:meta', {FILTER=>"PrefixFilter('$table')", COLUMNS=>['info:regioninfo']}
EOF

	cat $working_dir/find_table_meta.out|grep $table |grep -v "scan 'hbase:meta'" > $working_dir/find_table_meta.out_tmp
	mv $working_dir/find_table_meta.out_tmp $working_dir/find_table_meta.out
}

find_region_size(){
	table=$1
	hdfs dfs -du  /hbase2/data/default/$table | sort -n -k 1|grep -Ev 'tmp|tabledesc' > $working_dir/find_region_size.out
}

check_major_compacting(){
	>/tmp/check_major_compacting_ap.tmp
	curl -s 'http://10.104.9.27:16010/master-status' |grep 'href=table.jsp?name='|cut -d'=' -f3|cut -d'>' -f1  |while read table
	do
		curl -s "http://10.104.9.27:16010/table.jsp?name=${table}" |grep '<td>Enabled</td>' -A10|tr -d '\n'| awk -F '<td>|</td>' -v table=$table '{if($4=="true")print table" : " $10}' >> /tmp/check_major_compacting_ap.tmp
	done 
	count_major=`grep -ic major /tmp/check_major_compacting_ap.tmp`
	if [ $cont_major -lt 2  ]
		then
			echo " $count_major major compacting is happening now"
			return 0
		else
			echo " $count_major major compacting is happening now [WARN]"
			return 2
	fi
}




#find_table_meta RAW_5_20211213_20211219
#find_region_size RAW_5_20211213_20211219

find_merge()
{
	cat $working_dir/find_region_size.out|awk -v max=$min_size '{if($1<max)print $2}'|awk -F '/' '{print $NF}'|while read region
	do
	
		region_size=`grep $region $working_dir/find_region_size.out |awk '{print $1}'`
		pre_region=`grep ${region} $working_dir/find_table_meta.out -B1|cut -d. -f2|head -1`
		post_region=`grep ${region} $working_dir/find_table_meta.out -A1|cut -d. -f2|tail -1`
		pre_region_size=`grep $pre_region $working_dir/find_region_size.out |awk '{print $1}'`
		post_region_size=`grep $post_region $working_dir/find_region_size.out |awk '{print $1}'`
		if [ $region = $post_region ]	
		then
			merge_region=$pre_region
			merge_region_size=$pre_region_size
		else
			merge_region=$post_region
			merge_region_size=$post_region_size
		fi

		grep -E "$region|$merge_region" $working_dir/merge_out
		if [ $? -eq 1 ]
		then
			if [ $max_size -le $merge_region_size  ]
			then
				printlog "adding $region','$merge_region"
				echo " `echo $region_size|numfmt --to=iec` | merge_region '$region','$merge_region'| `echo $merge_region_size |numfmt --to=iec`" >> $working_dir/merge_out
			else
				printlog "skipping $region','$merge_region"
			fi
		fi

		#echo "$pre_region_size | $pre_region - $region - $post_region | $post_region_size "
		unset pre_region post_region pre_region_size post_region_size
	done
}

cat hbase_tunne.cfg|while read tables_det
do
	table_merge=`echo $tables_det | cut -d, -f1`
	min_size=`echo $tables_det | cut -d, -f2|numfmt --from=iec`
	max_size=`echo $tables_det | cut -d, -f3|numfmt --from=iec`
	find_merge
done

#cat $working_dir/merge_out
#check_major_compacting
echo $?
