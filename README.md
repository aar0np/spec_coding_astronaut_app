# Workshop - Spec coding a simple Astronaut application

## Workshop Contents
1. [Create your Database](#1-create-your-astra-db-database)
2. [Create schema](#2-create-your-schema)
3. [Set up the Astra DB connection details](#3-connect-to-astra-db)
4. [Load data](#4-load-data)
5. [Building the service layer](#5-building-the-service-layer)

## 1. Create your Astra DB Database

You can skip to step 1b if you already have an Astra DB database.

### 1a. Create Astra DB account

If you do not have an account yet, register and sign in to Astra DB at [astra.datastax.com](https://astra.datastax.com). This is FREE and NO CREDIT CARD is required. You can use your `GitHub` or `Google` accounts, or you can register with an email.

Follow this [guide](https://docs.datastax.com/en/astra-db-serverless/databases/create-database.html) or the steps below to create a free, **Serverless (Vector) database**.

 - Click on the "Create database" button.
 - Select "Serverless (vector)".
 - Enter a name for the database. _You can name it whatever you like._
 - Pick a cloud provider.
 - Select a "free" region close to where you are located.
 - Click "Create Database."

### 1b. Obtain your Astra DB credentials

 - At the next screen, be sure to click the **"Generate Token"** button, copy your new token, and paste it somewhere safe (for now).
 - Once the database is created, you will also need to copy the `API_ENDPOINT` value from the very top of the screen.
 - From the "three dots" menu on the right side of the screen (by "Region"), select "Download SCB" and save the file locally.
 
### 1c. Create your Astra DB keyspace

From the Data Explorer tab:
  - Click on the "Keyspace" menu.
  - Select "Create keyspace" and name it `astronaut_app`.

## 2. Create your schema

From the database dashboard, click on the "CQL console" button (top, upper-right). This will open a new tab in your browser.

Copy the following CQL statement into the CQL console:

```SQL
use astronaut_app;

CREATE TABLE astronauts (
    name TEXT PRIMARY KEY,
    dob DATE,
    birthplace TEXT,
    university_name TEXT);

CREATE TABLE astronauts_by_mission (
    astronaut_name TEXT,
    ship_name TEXT,
    mission_name TEXT,
    mission_start_date TIMESTAMP,
    mission_end_date TIMESTAMP,
    PRIMARY KEY(mission_name, astronaut_name));
```

_Note: These table definitions can also be found in the `data/astronaut_schema.cql` file._

## 3. Connect to Astra DB

For these next exercise steps, be sure to `cd` back into this project's (`kv-be-python-fastapi-dataapi-table`) directiory.

If you haven't already done so, install `astrapy` and `poetry`:

```bash
pip install astrapy
pip install poetry
```

_Note: Your AI coding agent will decide whether or not to use Poetry for dependency management. If you prefer, you can mention it in your prompt for creating the design doc ([step 5b](#5b-prompt-for-generating-the-design-doc-and-openapi-spec))._

Be sure to copy the `.env.example` file to `.env` and update the values in the `.env` file with your Astra DB instance.

```bash
cp .env.example .env
```

| Variable | Description |
|----------|-------------|
| `ASTRA_DB_API_ENDPOINT` | API endpoint for your Astra DB instance. This is the URL you use to access your Astra DB instance. |
| `ASTRA_DB_APPLICATION_TOKEN` | Token created in the Astra UI. |
| `ASTRA_DB_KEYSPACE` | `astronaut_app` |

## 4. Load data

### 4a. Load the astronauts table

The [astronauts.csv](data/astronauts.csv) is inside the `data/` directory, and can be loaded with the following command:

```
python3 loaders/load_astronauts.py
```

### 4b. Load the astronauts_by_mission table

The [astronauts_by_mission.csv](data/astronauts_by_mission.csv) is inside the `data/` directory, and can be loaded with the following command:

```
python3 loaders/load_astronauts_by_mission.py
```

## 5. Building the service layer
To build the application service layer, we are going to use an approach called "Spec Coding." For this to work properly, we are going to need to use an IDE with an AI coding agent, like [IBM Bob](https://bob.ibm.com/), Claude Code, or Cursor.

Basically, this approach is a multi-step process where we tell our coding agent to first generate our **requirements doc**, and from there both a **design doc** and an **OpenAPI specification file**. From there, we can ask our coding agent to generate a **TODO list** of all the tasks that need to be completed, and then, finally have it step through each task and generate the code for us.

### Sample prompts

### 5a. Prompt for generating the requirements doc
_I am building a service layer project that exposes restful endpoints to create, read, update, and delete astronaut data in an Astra database. Please help me build a requirements.md document for this project with numbered IDs, acceptence criteria for each, personas and user flows. At this point, we will not be focusing on implementation details or technology choices._

### 5b. Prompt for generating the design doc and OpenAPI spec
_Read the requirements.md document. From those requirements:_
1. _Create a design.md file with data model and tables (using schema from the data/astronaut_schema.cql file)._
2. _Inside the design.md file, detail the creation of a Python backend service layer using FastAPI, which allows for CRUD operations on the Astra database (referencing the .env file for the Astra database connection details). Use poetry for Python dependency management. Be sure that each endpoint maps back to a REQ-ID._
3. _Also in the design.md file, detail the creation of Python data loader scripts which read the `data/astronauts.csv` and `data/astronauts_by_mission.csv` files and loads them into the Astra database (referencing the `.env` file for the Astra database connection details)._
4. _Create an OpenAPI.yaml (OpenAPI 3.1) file that describes the endpoints and error responses of the FastAPI backend service layer._

### 5c. Prompt for generating the TODO list
_Read the requirements.md, design.md, and OpenAPI.yaml. From those requirements create a todo.md file that breaks down the tasks required to complete the project. Each of these tasks should be able to be marked done when complete._
