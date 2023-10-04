aws s3 mv \ 
    s3://pnp-data-lake-prod-acz-pos-euw1/bw/td/hprcw01/processed/ \
    s3://pnp-data-lake-prod-acz-pos-euw1/bw/td/hprcw01/ --recursive --dryrun | \
        egrep "*BW_HPRCW0122_..._2021100[5-9][^_]*" | \

                s3://pnp-data-lake-prod-acz-pos-euw1/bw/td/hprcw01/ --dryrun

#    s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hravm01/processed/$(date +%Y%m%d)/ \
aws s3 mv \
    s3://pnp-data-lake-prod-acz-pos-euw1/bw/td/hprcw01/history/ \
    s3://pnp-data-lake-prod-acz-pos-euw1/bw/td/hprcw01/ \
        --recursive \
        --exclude "*" --include "*BW_HPRCW012._..._.*_20211025[0-9]{6}.csv" \
        --include "*BW_HPRCW0121_*2020061*" \
        --include "*BW_HPRCW0121_*2020060*" \
        --dryrun | \
        tr -s ' ' | \
        cut -d' ' -f3,5 | \
        parallel -j0 --col-sep ' ' \
            aws s3 mv {1} {2} --dryrun

aws s3 ls \
    s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hravm01/to_process/

aws s3 mv \
    s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hravm01/to_process/ \
    s3://pnp-data-lake-prod-acz-retail-euw1/bw/td/hravm01/processed/$(date +%Y%m%d)/ \
        --recursive \
        --dryrun | \
        tr -s ' ' | \
        cut -d' ' -f3,5 | \
        parallel -j0 --col-sep ' ' \
            aws s3 mv {1} {2} --dryrun 

aws s3 ls s3://pnp-data-lake-prod-acz-pos-euw1/bw/td/hprcw01/history/ --recursive --profile pnp-emr-1 | \
    egrep 'BW_HPRCW012._..._.*_20211025[0-9]{6}.csv' | \
    tr -s ' ' | \
    cut -d' ' -f4 | \
    parallel -j0 \
        aws s3 mv \
            s3://pnp-data-lake-prod-acz-pos-euw1/bw/td/hprcw01/history/{} \
            s3://pnp-data-lake-prod-acz-pos-euw1/bw/td/hprcw01/ \
            --profile pnp-emr-1 --dryrun

parallel -j0 aws s3 mv 's3://pnp-data-lake-prod-acz-pos-euw1/bw/td/hprcw01/processed/20211031/{}' 's3://pnp-data-lake-prod-acz-pos-euw1/bw/td/hprcw01/{}' ::: BW_HPRCW0121_FUL_20200623_20211025063013.csv BW_HPRCW0121_FUL_20200626_20211025063128.csv BW_HPRCW0122_DLT_20211031014320.csv

aws s3 mv \
            s3://pnp-data-lake-prod-acz-pos-euw1/bw/td/hprcw01/history/ \
            s3://pnp-data-lake-prod-acz-pos-euw1/bw/td/hprcw01/ \
            --recursive \
            --exclude "*" \
            --include "*BW_HPRCW012*_2021102[56]*.csv" \
            --dryrun | \
            tr -s ' ' |         cut -d' ' -f3,5 |   parallel -j0 --silent --col-sep ' '             aws s3 mv {1} {2}