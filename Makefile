PROJECT_NAME := pnp-data-lake

ENVIRONMENT ?= development
ENVIRONMENT_SHORT ?= dev
REGION_SHORT ?= euw1
AWS_REGIONS ?= $(subst ., ,"eu-west-1")


S3_BUCKET := pnp-data-lake-${ENVIRONMENT_SHORT}-etl-scripts-${REGION_SHORT}
AWS_PROFILE_NAME := ${PROJECT_NAME}-platform-terraform

clean:
	rm -f src/emr/lib/pnp.zip

build_emr:
	cd src/emr/lib && zip -r pnp.zip pnp -x *.pyc -x __pycache__ && cd - 

build_glue:
	cd src/glue/transform_csv && python3 setup.py bdist_wheel

copy:
	for region in $(AWS_REGIONS); do \
		aws s3 sync src/glue s3://${S3_BUCKET}/glue/ --profile $(AWS_PROFILE_NAME); \
		aws s3 sync src/emr s3://${S3_BUCKET}/emr/ --profile $(AWS_PROFILE_NAME); \
		aws s3 sync src/libs s3://${S3_BUCKET}/libs/ --profile $(AWS_PROFILE_NAME); \
		aws s3 sync src/glue/transform_csv/dist/ s3://${S3_BUCKET}/libs/ --profile $(AWS_PROFILE_NAME); \
	done

aws_assume_role:
	chmod +x assume_role.sh
	./assume_role.sh ${CLIENT_ROLE_ARN} $(AWS_PROFILE_NAME)
