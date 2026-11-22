cd ~/Project/data/"Final Dataset"
rm -rf ~/hue_out && mkdir -p ~/hue_out

for f in $(ls consumption_*.txt | sort -t_ -k2 -n); do
	  echo "processing $f"
	    awk -v out="$HOME/hue_out" 'BEGIN{OFS="\t"; hdr="LOG_ID\tHOUSE_ID\tCONDATE\tCONHOUR\tENERGY_READING\tFLAG"}
	        FNR==1 {next}
		    { split($3, d, "-"); dir = out "/" d[1] "/" d[2] "/" d[3]
			          file = dir "/datafile_" d[1] "_" d[2] "_" d[3] ".txt"
				        if (file != cur) {
						        if (cur != "") close(cur)
								        system("mkdir -p " dir)
									        if (system("test -e " file) != 0) print hdr > file
											        close(file); cur = file
												      }
											            $1 = $1; print >> cur }' "$f"
											    done

											    hdfs dfs -mkdir -p /energyDataset
											    hdfs dfs -put ~/hue_out/* /energyDataset/
