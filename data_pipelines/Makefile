full_ingest:
	python3 source/full_copy_from_csv.py  ingest_web_rdbms.yaml
	python3 source/full_copy_from_csv.py  ingest_letter_rdbms.yaml


incremental_ingest:
	python3 source/incremental_copy_from_csv.py  ingest_web_rdbms.yaml
	python3 source/incremental_copy_from_csv.py  ingest_letter_rdbms.yaml

cleansing:
	python3 source/make_cleansing.py  cleansing_web.yaml
	python3 source/make_cleansing.py  cleansing_letter.yaml