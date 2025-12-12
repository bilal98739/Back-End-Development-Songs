from . import app
import os
import json
import pymongo
from flask import jsonify, request, make_response, abort, url_for  # noqa; F401
from pymongo import MongoClient
from bson import json_util
from pymongo.errors import OperationFailure
from pymongo.results import InsertOneResult
from bson.objectid import ObjectId
import sys

SITE_ROOT = os.path.realpath(os.path.dirname(__file__))
json_url = os.path.join(SITE_ROOT, "data", "songs.json")
songs_list: list = json.load(open(json_url))

# client = MongoClient(
#     f"mongodb://{app.config['MONGO_USERNAME']}:{app.config['MONGO_PASSWORD']}@localhost")
mongodb_service = os.environ.get('MONGODB_SERVICE')
mongodb_username = os.environ.get('MONGODB_USERNAME')
mongodb_password = os.environ.get('MONGODB_PASSWORD')
mongodb_port = os.environ.get('MONGODB_PORT')

print(f'The value of MONGODB_SERVICE is: {mongodb_service}')

if mongodb_service == None:
    app.logger.error('Missing MongoDB server in the MONGODB_SERVICE variable')
    # abort(500, 'Missing MongoDB server in the MONGODB_SERVICE variable')
    sys.exit(1)

if mongodb_username and mongodb_password:
    url = f"mongodb://{mongodb_username}:{mongodb_password}@{mongodb_service}"
else:
    url = f"mongodb://{mongodb_service}"


print(f"connecting to url: {url}")

try:
    client = MongoClient(url)
except OperationFailure as e:
    app.logger.error(f"Authentication error: {str(e)}")

db = client.songs
db.songs.drop()
db.songs.insert_many(songs_list)

def parse_json(data):
    return json.loads(json_util.dumps(data))

######################################################################
# INSERT CODE HERE
######################################################################
@app.route("/health", methods=["GET"])
def health_check():
    """
    Returns a simple status to indicate the service is up and running.
    """
    return jsonify({"status": "OK"})

@app.route("/count", methods=["GET"])
def count():
    """Queries the database and returns the total count of songs."""
    
    # Use the count_documents method on the 'songs' collection
    try:
        # {} is the filter document, counting ALL documents in the collection
        count = db.songs.count_documents({}) 
        
        # Insert HTTP OKAY response code (200) and return as JSON
        # The jsonify wrapper is usually preferred over returning a dict and status code
        return jsonify({"count": count}), 200
        
    except Exception as e:
        # Basic error handling for database connection issues
        print(f"Database error: {e}") 
        return jsonify({"error": "Failed to retrieve count"}), 500

# In backend/routes.py (add this function)

@app.route("/song", methods=["GET"])
def songs():
    """
    Handles the GET /song endpoint to retrieve all songs from the database.
    """
    try:
        # Use db.songs.find({}) to get a cursor to all documents
        song_cursor = db.songs.find({})
        
        # Convert the MongoDB cursor result into a list of dictionaries.
        # Use the existing 'parse_json' function to safely handle BSON types (like ObjectId).
        songs_list = [parse_json(song) for song in song_cursor]
        
        # Return the data wrapped in {"songs": list} and HTTP 200 OK
        return jsonify({"songs": songs_list}), 200

    except Exception as e:
        # Handle database connection or other exceptions
        app.logger.error(f"Error retrieving songs: {e}")
        return jsonify({"error": "Failed to retrieve songs"}), 500

@app.route("/song/<int:id>", methods=["GET"])
def get_song_by_id(id):
    """
    Handles the GET /song/<id> endpoint to retrieve a single song by its 'id' field.
    """
    try:
        # 1. Use db.songs.find_one({"id": id}) to find a single document
        # Note: We need to cast the 'id' from the URL (which is a string)
        # to an integer if the 'id' field in your MongoDB document is an integer.
        # The Flask route decorator <int:id> handles this casting automatically.
        
        song = db.songs.find_one({"id": id})
        
        # 2. Check if a song was found
        if song:
            # 3. If found, use parse_json and return with HTTP 200 OK
            return jsonify(parse_json(song)), 200
        else:
            # 4. If not found, return the 404 error message
            # Using jsonify to ensure the message is correctly formatted as JSON
            return jsonify({"message": f"song with id {id} not found"}), 404

    except Exception as e:
        # Handle database connection errors or other exceptions
        app.logger.error(f"Error retrieving song by ID {id}: {e}")
        return jsonify({"error": "An internal server error occurred"}), 500

@app.route("/song", methods=["POST"])
def create_song():
    """
    Handles the POST /song endpoint to insert a new song into the database.
    Checks for duplicates based on the 'id' field before insertion.
    """
    try:
        # 1. Extract the song data from the request body
        new_song = request.get_json()
        
        if new_song is None or 'id' not in new_song:
            # Simple check for missing data or required 'id'
            return jsonify({"error": "Invalid or missing song data"}), 400

        song_id = new_song.get('id')

        # 2. Check if a song with the id already exists
        # PyMongo find_one is used for the pre-insertion check
        existing_song = db.songs.find_one({"id": song_id}) 

        if existing_song:
            # 3. If a song is found, return HTTP 302 FOUND
            return jsonify({"Message": f"song with id {song_id} already present"}), 302
        else:
            # 4. If no duplicate, insert the new song
            # insert_one returns an InsertOneResult object
            result = db.songs.insert_one(new_song) 
            
            # 5. Return the inserted document's ObjectId with HTTP 201 CREATED
            # Use parse_json or json_util.dumps to correctly format the ObjectId
            inserted_id_json = json_util.dumps({"inserted id": result.inserted_id})
            
            # Flask's make_response is often helpful for complex JSON responses, 
            # but for this simple task, we can use the result of dumps:
            return inserted_id_json, 201, {'Content-Type': 'application/json'}
            # Alternative (simpler): return jsonify({"inserted id": parse_json(result.inserted_id)}), 201

    except Exception as e:
        # Handle exceptions (e.g., database failure)
        app.logger.error(f"Error creating song: {e}")
        return jsonify({"error": "An internal server error occurred during creation"}), 500

# In backend/routes.py (add this function)

@app.route("/song/<int:id>", methods=["PUT"])
def update_song(id):
    """
    Handles the PUT /song/<id> endpoint to update an existing song by its 'id' field.
    """
    try:
        # 1. Extract the update data from the request body
        update_data = request.get_json()
        
        if update_data is None:
            return jsonify({"error": "Missing update data in request body"}), 400
        
        # Prevent the user from trying to change the primary identifier (id field)
        update_data.pop('id', None) 
        
        # 2. Use db.songs.update_one to find the song and apply changes
        # Filter: {"id": id}
        # Update operation: {"$set": update_data}
        result = db.songs.update_one({"id": id}, {"$set": update_data}) 

        # 3. Check the result of the update operation
        if result.matched_count == 0:
            # Song not found
            return jsonify({"message": "song not found"}), 404
        
        elif result.modified_count == 0:
            # Song found, but no fields were changed (e.g., sending the exact same data)
            return jsonify({"message": "song found, but nothing updated"}), 200
        
        else:
            # Song found and updated successfully (modified_count > 0)
            
            # Per your specific requirement, the first successful update returns the 
            # updated document and status 201 CREATED. We need to fetch the updated document.
            
            # Fetch the newly updated song
            updated_song = db.songs.find_one({"id": id})
            
            # Return the updated song as JSON with HTTP 201 CREATED
            return jsonify(parse_json(updated_song)), 201 

    except Exception as e:
        app.logger.error(f"Error updating song by ID {id}: {e}")
        return jsonify({"error": "An internal server error occurred during update"}), 500  


