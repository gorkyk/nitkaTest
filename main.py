from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
import yaml
from motor.motor_asyncio import AsyncIOMotorClient

app = FastAPI()

# MongoDB configuration
client = AsyncIOMotorClient('mongodb://localhost:27017')
db = client['configurations_db']
configurations_collection = db['configurations']
tables_collection = db['tables']


class Table(BaseModel):
    type: str
    database_name: str
    table_name: str


class Configuration(BaseModel):
    filename: str
    step_name: str
    service_name: str
    service_config: Dict[str, Any]


def add_table(tables, seen_tables, table_type, db_name, tbl_name):
    """Helper function to add table to the list if it's not already seen."""
    if db_name and tbl_name and isinstance(db_name, str) and isinstance(tbl_name, str):
        table_key = (table_type, db_name, tbl_name)
        if table_key not in seen_tables:
            tables.append(Table(type=table_type, database_name=db_name, table_name=tbl_name))
            seen_tables.add(table_key)


def extract_tables_from_config(service_config: Dict[str, Any], tables: List[Table], seen_tables: set):
    """Recursively extract tables from the configuration and avoid duplicates."""
    if isinstance(service_config, dict):
        add_table(tables, seen_tables, "source", service_config.get("source_database"),
                  service_config.get("source_table"))
        add_table(tables, seen_tables, "target", service_config.get("target_database"),
                  service_config.get("target_table"))
        add_table(tables, seen_tables, "target", service_config.get("landing_database"),
                  service_config.get("landing_table"))
        add_table(tables, seen_tables, "target", service_config.get("database_name"), service_config.get("table_name"))

        refinery_db = service_config.get("refinery_database")
        refinery_tables = service_config.get("refinery_tables")
        if refinery_db and refinery_tables and isinstance(refinery_tables, list):
            for refinery_table in refinery_tables:
                if isinstance(refinery_table, dict):
                    for table in refinery_table.values():
                        add_table(tables, seen_tables, "source", refinery_db, table.get("table_name"))

        for key, value in service_config.items():
            if isinstance(value, (dict, list)):
                extract_tables_from_config(value, tables, seen_tables)
    elif isinstance(service_config, list):
        for item in service_config:
            extract_tables_from_config(item, tables, seen_tables)


async def save_tables(filename: str, service_config: Dict[str, Any]):
    tables = []
    seen_tables = set()
    extract_tables_from_config(service_config, tables, seen_tables)

    # Delete existing tables for the configuration filename
    await tables_collection.delete_many({"filename": filename})

    # Insert new tables
    for table in tables:
        await tables_collection.insert_one({"filename": filename, **table.dict()})


@app.post("/upload/")
async def upload_configuration(file: UploadFile = File(...)):
    try:
        contents = await file.read()
        config_data = yaml.safe_load(contents)
        job_step = config_data['job_step']

        filename = file.filename
        configuration = Configuration(
            filename=filename,
            step_name=job_step['step_name'],
            service_name=job_step['service_name'],
            service_config=job_step['service_config']
        )

        # Delete the existing configuration and tables if they exist
        await configurations_collection.delete_one({"filename": filename})
        await tables_collection.delete_many({"filename": filename})

        # Insert the new configuration
        await configurations_collection.replace_one(
            {"filename": filename},
            {"filename": filename, **configuration.dict()},
            upsert=True
        )
        await save_tables(filename, job_step['service_config'])

        return {"message": "Configuration uploaded and replaced successfully"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/configuration/{filename}/tables")
async def get_tables(filename: str):
    configuration = await configurations_collection.find_one({"filename": filename})
    if not configuration:
        raise HTTPException(status_code=404, detail="Configuration not found")

    tables = await tables_collection.find({"filename": filename}).to_list(length=None)
    # Remove _id field from the documents
    for table in tables:
        if "_id" in table:
            del table["_id"]
    return {"tables": tables}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
