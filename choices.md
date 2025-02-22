Re-used code given in course where possible to reduce dev time.

Implemented more Airflow variables for practice:
    * raw_files_folder
    * clean_data_folder

Divided the DAG into four stages:
    * EXTRACT - pull the data from openweathermap.org
    * TRANSFORM - transform the raw data to clean data
    * SCORE - score each model on the clean data
    * SELECT - select and save the best model based on those scores

* EXTRACT - task "EXTRACT" as "extract"
    * carried out by the fucntion "extract_data"
    * uses the Airflow variable "cities" in a for loop to perform an api get for each city to get the weather data for that city
    * stores this data in a li!st "cities_data"
    * uses the Airflow variable "raw_files_folder" as location to save that data list
    * this stage is done with a single task, "EXTRACT"

* TRANSFORM - task group "TRANSFORM" as "transform"
    * takes the raw data from the previous stage EXTRACT and transfroms it using the code supplied as the function "transform_data"
    * uses the Airflow variable "raw_files_folder" as source directory of raw data
    * uses the Airflow variable "clean_data_folder" as destination directory of processed data
    * this stage is performed as a task group with two tasks:
        * TRANSFORM_2 for feeding the Dashboard
        * TRANSFORM_3 for training the models
    * To avoid boilerplate, I have opted to save the features and target for later use, in functions/tasks: 
        * "compute_model_score"/SCORE, and
        * "train_and_save_model"/SELECT


* SCORE - task "SCORE" as "score"
    * this stage is implemented as a task group with four tasks
    * using a for loop, a list of four tasks is constructed, one for each model
    * each task in this group calls the "compute_model_score" function to be  score a particular member of the models list found in the Airflow variable "models"
    * the score results are stored in xcoms

* SELECT - task group "SELECT" as "select"
    * implemented in two steps
        * first the scores are retrieved from xcoms for comparison using the function "evaluate_models_and_select"
        * the selected "best_model" is then passed as a parameter to the function "train_and_save_model" which trains the model and saves it a pickle file


Tasks are ordered as follows

* extract task
then
* transform task group - 2 tasks exectuted in parallel
then
* score task group - 4 tasks exectuted in parallel
then
* select task