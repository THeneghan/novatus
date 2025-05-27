
## Installation and Usage
The user is recommended to use poetry to run this project. A requirements.txt file has been provided (auto generated) by poetry but this has not been tested.
Docker is also required as this project creates a postgres instance in a docker container.

Connection details are as follows - it is recommended that the user accesses the data using a client e.g. DBeaver, to view the data.

Host: localhost

User: postgres

Password: password

To install all the required dependencies and ensure pre-commit is installed, run:

```bash
poetry install
 
pre-commit install
   
```

## Assumptions and trade offs
This approach randomly generates data to prove the logic described. 
I have largely avoided using the config.yaml as I am largely against using them in SQL related work due to logic changes propagating in an unexpected way.
SQLalchemy has been used which can often be slower than using raw sql, it does how provide in my opinion greater configurability for business logic and readability. Write operations in practice are often switched out for faster/less readable approaches (String Buffer approach etc.)
## Running

Three files should be ran in the order displayed in the bash script below.

_src/main.py_ creates the postgres container, schema, data etc and will print out a reporting json. Repetitive running will cause Primary Key errors or table already exists.

_src/novatus_api/main.py_ starts the proof of concept Fast API service, leave this running to use it to interact with the API.

_python src/api_demo.py_ # will send a JSON to the API which will be written to the customer transaction table (assume both previous scripts have been ran)
```bash
python src/main.py 
python src/novatus_api/main.py 
python src/api_demo.py 

   
```


## Troubleshooting

Some IDEs, such as PyCharm, may struggle to identify modules elsewhere in this repository. Make sure to mark the `src` directory as 'Sources Root' by right-clicking the folder and selecting `Mark Directory as Sources Root`.

## Testing

Pytest has been used for testing - this generates a testing docker container which runs the tests in a separate docker container to that created in src/main.py

```bash
pytest
    
```