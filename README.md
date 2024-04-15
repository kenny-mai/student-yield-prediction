Subfolder for Yield Modelling

## Steps to Reproduce

### Virtual Enviroment

After cloning the repo, you will need to set up the virtual enviroment and install dependencies by running the following commands in the CLI in the folder:

* python3 -m venv .venv
* source .venv/bin/activate
* pip install -r requirements.txt
* pip install ../shared_packages/aws_helper_functions/

### Setting Enviroment Vars & AWS Config

If run outside of lambda, applicable functions must be called with local_mode=True. Enviroment variables must be set for `host`, `database`, `port`, `username`, and `password` (eg redshift password) to connect to redshift. If writting results to S3 to update tables, AWS config must be set up w/`access key` and `secret access key`. 

## Files & Usage

* `yield_boosting.py` -> entry point
  * `train_basetable.sql` -> query to create training data set
  * `eval_basetable.sql` -> query to create evaluation data set
* `requirements.txt` -> packages that are requried to run intra_year_boosting (other than `aws_helper_functions`)