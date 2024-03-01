from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine, Column, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import csv
from datetime import datetime
from werkzeug.exceptions import BadRequest
import os

app = Flask(__name__)
Base = declarative_base()

# Define Custom Error Class
class CustomError(Exception):
    def __init__(self, message, status_code):
        super().__init__(self)
        self.message = message
        self.status_code = status_code

# Define CardStatus ORM model
class CardStatus(Base):
    __tablename__ = 'card_status'
    id = Column(String, primary_key=True)
    card_id = Column(String)
    phone = Column(String)
    timestamp = Column(DateTime)
    status = Column(String)

# Check if the database file exists before creating the engine
if not os.path.exists('card_status.db'):
    engine = create_engine('sqlite:///card_status.db')
    Base.metadata.create_all(engine)
else:
    engine = create_engine('sqlite:///card_status.db')

Session = sessionmaker(bind=engine)

def add_latest_timestamp(session, row):
    id_value = row.card_id
    existing_row = session.query(CardStatus).filter_by(card_id=id_value).first()
    if existing_row:
        # Compare timestamps
        if existing_row.timestamp < row.timestamp:
            # Delete the existing row
            session.delete(existing_row)
            session.add(row)
            session.commit()  # Commit the deletion

    else:
        # Add the current row to the table
        session.add(row)
        session.commit()  # Commit the addition


    

# Function to load data from CSV files into database
def load_csv_to_database():
    session = Session()
    for file_name, status in [('Pickup.csv', 'pickup'), ('Delivered.csv', 'delivered'), ('Delivery exceptions.csv', 'redelivery'), ('Returned.csv', 'returned')]:
        with open(f'data/Sample Card Status Info - {file_name}', 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                phone_number = row.get('User contact') if 'User contact' in row else row.get('User Mobile')
                phone_number =  phone_number.replace('"', '')
                timestamp = None
                if 'Timestamp' in row:
                    if 'Pickup' in file_name:
                        timestamp_format = '%d-%m-%Y %H:%M %p'
                    elif 'Delivered' in file_name:
                        timestamp_format = '%Y-%m-%dT%H:%M:%SZ'
                    elif 'Returned' in file_name:
                        timestamp_format = '%d-%m-%Y %I:%M%p'
                    else:
                        timestamp_format = '%d-%m-%Y %H:%M'
                    
                    timestamp = datetime.strptime(row.get('Timestamp'), timestamp_format)

                if not row.get('ID'):
                    break

                card_status = CardStatus(
                    id=row.get('ID'),
                    card_id=row.get('Card ID'),
                    phone=phone_number,
                    timestamp=timestamp,
                    status=row.get('Comment') if 'Comment' in row else status
                )
                
                add_latest_timestamp(session, card_status)
    session.commit()
    session.close()

# Load CSV data into database
try:
    load_csv_to_database()
except Exception as e:
    raise CustomError('Failed to load CSV data into database', 500)

# Custom error handler for CustomError
@app.errorhandler(CustomError)
def handle_custom_error(error):
    response = jsonify({'error': error.message})
    response.status_code = error.status_code
    return response

# Custom error handler for BadRequest (400)
@app.errorhandler(BadRequest)
def handle_bad_request(error):
    response = jsonify({'error': 'Bad request'})
    response.status_code = 400
    return response

@app.route('/get_card_status', methods=['GET'])
def get_card_status():
    # Validate request data
    data = request.json
 
    card_id = data.get('card_id')
    phone_number = data.get('phone_number')

    if not card_id and not phone_number:
        raise CustomError('EMPTY: Enter card_id or phone_number', 400)

    # Validate card_id format
    if card_id and not isinstance(card_id, str):
        raise CustomError('card_id must be a string', 400)

    # Validate phone_number format
    if phone_number and not isinstance(phone_number, str):
        raise CustomError('phone_number must be a string', 400)

    # Perform database query
    session = Session()

    if card_id:
        query = session.query(CardStatus).filter_by(card_id=card_id).order_by(CardStatus.timestamp.desc()).first()
    else:
        query = session.query(CardStatus).filter_by(phone=phone_number).order_by(CardStatus.timestamp.desc()).first()

    session.close()

    if query:
        return jsonify({'card_id': query.card_id, 'phone_number': query.phone, 'timestamp': query.timestamp.isoformat(), 'status': query.status})
    else:
        raise CustomError('No Match found, Kindly Enter correct data of card_id or phone_number', 404)

if __name__ == '__main__':
    app.run(debug=True)
