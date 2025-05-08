#!/bin/bash

# Ensure script is run from crushdb root directory

#Examples:
#
# createCrate Vehicle
# createIndex Vehicle String make_index vehicleMake false 3
#
# Vehicle = crate
# STRING = BsonType
# make_index = index name
# vehicle_make = field name
# false = not unique
# 3 = B-Tree order
#
# insertOne Vehicle {"_id": 1234567, "vehicle_make": "Subaru", "vehicle_model": "Forester", "vehicle_year": 2019, "vehicle_type": "automobile", "vehicle_body_style": "SUV", "vehicle_price": 28500.99, "hasHeating": true}
# find Vehicle {"vehicle_year": { "$gt": 2015 }}

# Paths to compiled class files
CLI_CP="cli/target/classes"
CORE_CP="core/target/classes"

# (Optional) additional dependencies
DEPS="cli/target/dependency/*"

# Run CLI with core and cli on the classpath - currently set for development
# This will change once CrushDB is packed and end user needs CLI & other tools
java -cp "$CLI_CP:$CORE_CP:$DEPS" com.crushdb.cli.CrushCLI "$@"

