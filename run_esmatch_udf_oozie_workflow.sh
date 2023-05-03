#!/bin/sh
if [ "$#" -ne 3 ]; then
   echo "Illegal number of parameters. There should be three parameters. Example ./run_esmatch_udf_oozie_workflow.sh <location> <hdfs> <unique_identifier>"
   exit 2
fi
#------User arguments--------#
location_path=$1  #/user/avenugopalan/zkpl_demo/input_file/
file_system=$2  #hdfs or local
unique_identifier=$3  #x001


#------Config Variables--------#
# To be done  - The below values has to be fetched from config.properties >> DONE
source config.properties
#jar_file="hdfs:///user/avenugopalan/zkpl_demo/ZKPL_ESMATCH_AV_APRIL20.jar"  #from properties
#delimeter="\t" #from properties
#echo $jar_file
#echo $delimeter

#Other dynamic variables
input_table_name="esmatch_input_csv_${unique_identifier}"
tsv_stage_location="$location_path/$unique_identifier/stage"

sed  "s|<<jar_location>>|$jar_file|g" $hql_filename > $temp_hql_filename
cat $temp_hql_filename > $hql_filename

sed  "s|<<delimeter>>|$delimeter|g" $hql_filename > $temp_hql_filename
cat $temp_hql_filename > $hql_filename

sed  "s|<<input_table_name>>|$input_table_name|g" $hql_filename > $temp_hql_filename
cat $temp_hql_filename > $hql_filename


if [ $file_system = "hdfs" ]; then
   echo "File system is hdfs."
   echo "Hdfs location is $location_path"
   echo "Downloading the file from $location_path to local current directory"
   # find the file name present inside the above directory using hadoop fs -ls command
   # only one file should be present, if not it should throw an exception and exit
   # After getting the file name , download it into current directiry using hadoop fs -get command
   #file_listed=$(hadoop fs -ls /user/avenugopalan/zkpl_demo/input_file/)
   file_listed="Found 1 items -rw-r--r-- 3 avenugopalan avenugopalan 1265 2023-05-03 01:44 /user/avenugopalan/zkpl_demo/input_file/tsv_sample.tsv"
   echo "hdfs listing : $file_listed"
   #file_path="/user/avenugopalan/zkpl_demo/input_file/tsv_sample.tsv" # This should be extracted from above full listing -To be done
   #file_name="tsv_sample.tsv" #This should be also derived from above path -To be done
   file_path=${file_listed##* } # This trims everything from the front until a '<space>'
   file_name=${file_path##*/}   # This trims everything from the front until a '/'
   echo "---Copying $file_name from hdfs to local"
   #hadoop fs -get $file_path .   # This command will copy the file to current directory
else
   echo "File system is local"
   # find the file name present inside the local directory
   # only one file should be present, if not it should throw an exception and exit
   #file_name="tsv_sample.tsv" 
   tsv_files=(`ls *.tsv`)
   if [ ${#tsv_files[@]} -ne 1 ]; then
      echo "only one file should be present"
      exit 2
   fi
   file_name=$tsv_files 
fi
alt_file_name="alt_$file_name"
echo "Adding a left most column in the file for line_no"
awk '{printf "%d\t%s\n", NR, $0}' < $file_name > $alt_file_name


echo "Copying the altered file $alt_file_name to hdfs stage location $tsv_stage_location: "
#hadoop fs -put $alt_file_name $tsv_stage_location


echo "Creating a staging hive table on top of $tsv_stage_location"
# Hive/beeline -f command for for creating a table


echo "Preparing the hql for oozie job (replacing the place holder)"
# To be done  - replace the place holders (<< >>) in esmtach_udf_zkpl_placeholder.hql


#oozie_id=$(oozie job -oozie http://drlvcdhchn01:11000/oozie/ -config job.properties -run)
echo "Ozzie Job is Submitted and Job id is : $oozie_id"

