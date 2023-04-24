# Stones_upd

Is a project to process data about purchases of stones provided in .csv files. 

## Installation

Before running the project, make sure you have [Docker](https://docs.docker.com/engine/install/) installed on your computer.

Create a seperate folder for the project and clone from github:

```bash
git clone https://github.com/praktikant101/stones_upd
```

## Set-up

Once you have cloned from the repository, create a .env file as per .env_example for the below-mentioned variables and set their values as you wish:

```bash
DJANGO_SECRET_KEY=

DB_NAME=
DB_USER=
DB_PASSWORD=
DB_PORT=
```

Add the following five parameters to your newly created .env file:

```bash
DB_HOST=db
DB_PORT=5432
DB_ENGINE=django.db.backends.postgresql
REDIS_HOST=redis
REDIS_PORT=6379
```

## Usage

To run the project for the first time, first, you will need to build it:

```bash
docker-compose up --build
```

Once the project is built, you can simply run the project by:

```bash
docker-compose up
```

You can find more information about docker-compose [here](https://docs.docker.com/get-started/08_using_compose/#:~:text=Docker%20Compose%20is%20a%20tool,or%20tear%20it%20all%20down.)





