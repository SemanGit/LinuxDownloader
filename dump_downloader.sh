#!/bin/bash
# Determin OS

help=false
skip_install=false
keep_data=false
output_dir=$(pwd)

jar_location=$(pwd)/Converter/out/artifacts/converter_main_jar/converter_main.jar

clear

echo "This program runs under following license:"
echo ""	
echo ""
echo ""
echo "-------------------------------------------"
echo "-------------------------------------------"
less -FX ./LICENSE
echo "-------------------------------------------"
echo "-------------------------------------------"
echo ""
echo ""
echo ""
echo "Please state:"
echo "I HAVE READ AND ACCEPT THE LICENSE AGREEMENT"
echo "by typing: yes"

read -r confirm
if [ "$confirm" != "yes" ]; then
	exit 499
fi
clear	




# Check options
for option in "$@"
do
	if [ "$option" == "-h" ] || [ "$option" == "-help" ]; then
		help=true
	elif [ "$option" == "-s" ] || [ "$option" == "-skip_install" ]; then
		skip_install=true
	elif [ "$option" == "-k" ] || [ "$option" == "-keep_data" ]; then
		keep_data=true 
	elif [ 	$( echo "$option" |cut -d '=' -f1 ) == "-output_dir" ]; then
		output_dir=$( echo "$option" |cut -d '=' -f2 )
		if [ ! -d "$output_dir" ]; then 
			echo ""
			echo "$output_dir not found. Please make sure it exists and your parameter fullfills follwing form: "
			echo "<folder>/.../<subfolder>"
			help=true		
		fi
	else 
		echo ""
		echo "Unkown option $option. Help will be automatically printed"
		help=true	
	fi
done

# Print help if required
if [ "$help" == true ]; then	
	clear
	echo "Printing help file:"
	echo ""
	echo 'Options:'
	echo '-------------------'
	echo '-skip_install, -s: 			Skip automatic installation of requirements'
	echo '-keep_everything, -k:			Keep intermediate .csv Datasets.'
	echo '-output_dir=<path>:			Write output files (RDF Files, Logs, control files) to <path>'
	echo '-------------------'
	echo -e '-h , -help:				Print help file'
else
	# Skip install if required
	if ( ! "$skip_install" == true ); then
		clear
		echo "Installing dependencies if required"
		echo "------------------"

		if ( ! $( which apt ) != ""  ) ; then 
			command="sudo apt-get install" 
			echo "Auto-detected: apt - package manager"
		elif ( ! $( which pacman ) != "" ) ; then 
			command="sudo pacman -S" 
			echo "Auto-detected: pacman - package manager"
		elif ( ! $( yum apt ) != "" ) ; then 
			command="sudo yum install" 
			echo "Auto-detected: yum - package manager"
		elif ( ! $( which zipper ) != "" ) ; then 
			command="sudo zipper install"
			echo "Auto-detected: zipper - package manager" 
		else  
			echo "no package-manager auto-detected. Please check if following dependencies are met: "
			echo "curl"
			echo "java"
			echo "pigz"
			break 
		fi
		echo "------------------"

		#Check dependencies 
		while   [ ! $( which curl ) != "" ]; do
			echo ""
			echo "Package curl not found. Do you want to execute:"
			echo "-------------------------"
			echo  $command " curl [Y/N]" 
			echo "-------------------------"
			echo "If you choose 'N' this programm will terminate and you have to install it manually"
			read confirm
			if [ "$confirm" == "Y" ]; then
				$command curl
				if  [ $(which curl) == '' ]; then
					echo "Something went wrong during the installation."		
				fi
			else 
				exit 499
			fi
		done;

		while  [ ! $( which pigz ) != "" ]; do
			echo ""
			echo "Package pigz not found. Do you want to execute:"
			echo "-------------------------"
			echo $command " pigz [Y/N]" 
			echo "-------------------------"
			echo "If you choose 'N' this programm will terminate and you have to install it manually"
			read confirm
			if [ "$confirm" == "Y" ]; then
				$command pigz
				if  [ $(which pigz) == '' ]; then
					echo "Something went wrong during the installation."		
				fi
			else 
				exit 499
			fi
		done;

		while  [ ! $( which java ) != "" ]; do
			echo ""
			echo "No java runtime environment found in the systems path variable. Do you want to execute:"
			echo "-------------------------"
			echo $command "default-jre [Y/N]" 
			echo "-------------------------"
			echo "If you choose 'N' this programm will terminate and you have to install it manually"
			read confirm
			if [ "$confirm" == "Y" ]; then
				$command default-jre
				if  [ $(which java) == '' ]; then
					echo "Something went wrong during the installation."		
				fi
			else 
				exit 499
			fi
		done;
	fi
	#Check for required folders etc, create log file in output_dir
	if [ ! -d "$output_dir/ghtorrent.com" ]; then
		mkdir "$output_dir/ghtorrent.com/"
	fi
	if [ ! -d "$output_dir/logs" ]; then
		mkdir "$output_dir/logs"
	fi

	timestamp=`date "+%Y_%m_%d_%H_%M_%S"`
	log="$output_dir/logs/log_$timestamp"
	
	#Download overview from ghtorrent.com
	clear
	curl http://ghtorrent.org/downloads.html > $output_dir/ghtorrent.com/ghtorrent_new
	grep 'http://ghtorrent-downloads.ewi.tudelft.nl/mysql' $output_dir/ghtorrent.com/ghtorrent_new > $output_dir/ghtorrent.com/ghtorrent_links
	curl_exit_code=$?
	if [ "$curl_exit_code" == "0" ]; then
		clear
		echo "Following files are available for Download:"
		echo "-------------------------"
		count_files=0
		while IFS= read -r line; do
				str_link=$( echo "$line" |cut -d '"' -f2 )
				str_file_name=$( echo "$str_link"| cut -s -d/ -f5)
				str_folder_name=$( echo "$str_file_name"| cut -s -d "." -f1)
				str_size=$( echo "$line"| cut -s -d "(" -f2 |cut -s -d " " -f1)
				if [[ ! (($str_folder_name =~ "2013") || ($str_folder_name =~ "2014") || ($str_folder_name =~ "2015")) ]]; then
					count_files=$((count_files+1))
					echo "$count_files) $str_file_name"
				fi
		done <<< "$(tac $output_dir/ghtorrent.com/ghtorrent_links)"
	else
		echo ""
		echo "An Error with curl occured. Please check your internet connection"
		exit 503
	fi

	# Choose dump to be downloaded
	echo "-------------------------"
	echo "Which dump do you want to download ? (Example: Type 13 for dump mysql-2017-01-19.tar.gz)"
    	read -r dump_no
	echo ""
	count_files=0
	found=false
	while IFS= read -r line; do
		str_link=$( echo "$line" |cut -d '"' -f2 )
		str_file_name=$( echo "$str_link"| cut -s -d/ -f5)
		str_folder_name=$( echo "$str_file_name"| cut -s -d "." -f1)
		str_size=$( echo "$line"| cut -s -d "(" -f2 |cut -s -d " " -f1)
		if [[ ! (($str_folder_name =~ "2013") || ($str_folder_name =~ "2014") || ($str_folder_name =~ "2015")) ]]; then
			count_files=$((count_files+1))
			if [[ "$dump_no" == "$count_files" ]]; then
				found=true
				break
			fi
		fi
	done <<< "$(tac $output_dir/ghtorrent.com/ghtorrent_links)"
	echo $found
	if [ "$found" == false ]; then
		echo "Input not recognized"
		exit 400
	fi
	
	# Ask user about confirmation for processing
	processing_size=$(expr $str_size \* 8 / 1024)
	final_size=$(expr $str_size \* 4 / 1024 )
	folder_available=$(expr $(df -k $output_dir | cut -d ' ' -f4) / 1024 / 1024)
	clear
	echo "For the processing of $str_file_name we estimate a requirement of:"
	echo "-------------------------"
	echo "Processing space:       $processing_size GiB "
	echo "Output Space:           $final_size GiB "
	echo "-------------------------"
	echo "The target directory has about $folder_available GiB of available space."
	echo ""
	echo ""
	echo "WARNINGS:"
	echo "- Having not enough available space for the processing, may harm your system"
	echo ""
	echo "NOTES:"
	echo "- The following steps consist of Downloading, Extracting and Transforming the data."
	echo "- In dependence of your download speed and system resources each step can take several hours"
	echo "- If you want to abort the script, the result of each uncompleted step is lost."
	echo "- If you rerun the dump_downloader, it will continue with the last successfull step"
	echo "- "

	echo "Do you really want to continue? [Y/N]"
	read -r confirm_processing

	if [ $confirm_processing != "Y" ]; then
		exit 499
	else
		#Prepare Folders and Files
		clear
		str_folder_name="$output_dir/$str_folder_name"
		if [ ! -d "$str_folder_name" ]; then
			mkdir "$str_folder_name"
			timestamp=`date "+%Y_%m_%d_%H_%M_%S"`
			echo "$timestamp --- Folder: $str_folder_name created" >> $log
		fi
		
		# Save information from the ghtorrent website in the folder
		echo "$str_link" > "$str_folder_name/SG_LINK"
		echo "$str_file_name" > "$str_folder_name/SG_FNAME"
		echo "$str_size" > "$str_folder_name/SG_FSIZE"

		# Check if file is already downlaoded
		if [ ! -f "$str_folder_name/SG_DOWNLOAD_DONE" ]; then
			timestamp=`date "+%Y_%m_%d_%H_%M_%S"`
			echo "$timestamp --- Folder: $str_folder_name no SG_DOWNLOAD_DONE" >> $log

			if [ -f "$str_folder_name/$str_file_name" ]; then
				rm $str_folder_name/$str_file_name
				timestamp=`date "+%Y_%m_%d_%H_%M_%S"`
				echo "$timestamp --- File: removing $str_folder_name/$str_file_name" >> $log
			fi

			curl -o "$str_folder_name/$str_file_name" "$str_link"
			timestamp=`date "+%Y_%m_%d_%H_%M_%S"` 
			echo "$timestamp --- File: Downloading $str_folder_name/$str_file_name" >> $log
			curl_exit_code=$?

			
			if [ "$curl_exit_code" == "0" ]; then
				
				timestamp=`date "+%Y_%m_%d_%H_%M_%S"`
				echo "$timestamp --- File: Downloading $str_folder_name/$str_file_name sucseeded" >> $log
				tar -tzf "$str_folder_name/$str_file_name" >/dev/null
				tar_exit_code=$?
				if [ "$tar_exit_code" == "0" ]; then
					timestamp=`date "+%Y_%m_%d_%H_%M_%S"`
					echo "" > "$str_folder_name/SG_DOWNLOAD_DONE"
					echo "$timestamp --- File: $str_folder_name/$str_file_name is extractable" >> $log
					echo "$timestamp --- File: Creating $str_folder_nam/SG_DOWNLOAD_DONE" >> $log
				fi
			fi
		else
			timestamp=`date "+%Y_%m_%d_%H_%M_%S"`
			echo "$timestamp --- File: $str_folder_name/$str_file_name already Downloaded" >> $log
		fi
		# Start processing if last processing did not suceeded
		if [  -f "$str_folder_name/SG_DOWNLOAD_DONE" ] && [ ! -f "$str_folder_name/SG_PROCESSING_DONE" ]; then

#--------------------------------------------------------------------
#--------------------------------------------------------------------
#--------------------------------------------------------------------

			#Check validity
			if [ -d "$str_folder_name" ] ; then
				cd "$str_folder_name"
				if [ -f "SG_PROCESSING_DONE" ]; then
					exit 0
				fi
				if [ -f "SG_DOWNLOAD_DONE" ] && [ ! -f "SG_UNPACKING_DONE" ]; then
					timestamp=`date "+%Y_%m_%d_%H_%M_%S"`
					echo "$timestamp --- Starting Extraction" >> $log
					tar -I pigz -xvf "$str_file_name" >> $log
					tar_exit_code=$?
					if [ "$tar_exit_code" == "0" ]; then
						echo "" > "SG_UNPACKING_DONE"
						timestamp=`date "+%Y_%m_%d_%H_%M_%S"`
						echo "$timestamp --- Unpacking finished successfully" >> $log
					else
						timestamp=`date "+%Y_%m_%d_%H_%M_%S"`		
						echo "$timestamp --- Tar exit code: $tar_exit_code" >> $log
						exit 3
					fi
				elif [ ! -f "SG_UNPACKING_DONE" ]; then
					echo "Downloaded-File not found. This message should never appear."
					exit 2
				fi
				if [ -f "SG_UNPACKING_DONE" ] ; then
					timestamp=`date "+%Y_%m_%d_%H_%M_%S"`
					echo "$timestamp --- Starting Conversion" >> $log
					for d in */ ; do
						java -jar "$jar_location" "$d" -debug >> $log 2>&1 ; java_exit_code=$? 
					done
					if [ "$java_exit_code" == "0" ]; then 
						timestamp=`date "+%Y_%m_%d_%H_%M_%S"`
						if [ "$keep_data" == false ]; then
							echo "$timestamp --- Translation sucseeded. Removing .csv and unused .ttl ." >> $log
							echo "$timestamp --- Translation sucseeded. Removing .csv and unused .ttl ." 
							rm **/* >/dev/null 2>&1
						else
							echo "$timestamp --- Translation sucseeded. Removing .ttl. Please remove all .csv manually if needed." >> $log
							echo "$timestamp --- Translation sucseeded. Removing .ttl. Please remove all .csv manually if needed." 
						fi
						echo "" > "SG_PROCESSING_DONE"
					elif [ "$java_exit_code" == "2" ]; then
						timestamp=`date "+%Y_%m_%d_%H_%M_%S"`		
						echo "$timestamp --- Translation sucseeded partially, as some input lines are corrupted. Please remove all .csv manually if needed."  >> $log
						echo "$timestamp --- Translation sucseeded partially, as some input lines are corrupted. Please remove all .csv manually if needed."
						echo "" > "SG_PROCESSING_DONE"
					else
						timestamp=`date "+%Y_%m_%d_%H_%M_%S"`		
						echo "$timestamp --- Translation did not sucseed. Check for Java Errors" >> $log
						echo "$timestamp --- Translation did not sucseed. Check for Java Errors" 
						exit 4
					fi

				fi
			else 
				echo "Faulty Parameters set."
				exit 1
			fi


#--------------------------------------------------------------------
#--------------------------------------------------------------------
#--------------------------------------------------------------------
		fi
	fi
fi


	




