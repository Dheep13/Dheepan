from flask import Flask, request, jsonify
import psycopg2
import psycopg2.extras

app = Flask(__name__)

# Database connection parameters
DB_HOST = "postgres-91e2dc78-2c46-4080-bfb4-b9a61750f576.cqryblsdrbcs.us-east-1.rds.amazonaws.com"
DB_NAME = "sCIaIIXGCJSb"
DB_USER = "66cca5aa982f"
DB_PASS = "4f7e7773940c82814f567e08a46f9e68"

# Establish a database connection
def get_db_connection():
    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    return conn

@app.route('/profiles', methods=['POST'])
def create_profile():
    conn = get_db_connection()
    cur = conn.cursor()
    profile_data = request.json
    cur.execute('INSERT INTO profiles (user_id, profile_data) VALUES (%s, %s) RETURNING *;',
                (profile_data['user_id'], json.dumps(profile_data['profile_data'])))
    profile_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return jsonify({'profile_id': profile_id}), 201

@app.route('/profiles/<int:user_id>', methods=['GET'])
def get_profile(user_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute('SELECT * FROM profiles WHERE user_id = %s;', (user_id,))
    profile = cur.fetchone()
    cur.close()
    conn.close()
    if profile is None:
        return jsonify({'error': 'Profile not found'}), 404
    return jsonify(dict(profile)), 200

@app.route('/profiles/<int:user_id>', methods=['PUT'])
def update_profile(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    profile_data = request.json
    cur.execute('UPDATE profiles SET profile_data = %s WHERE user_id = %s RETURNING *;',
                (json.dumps(profile_data['profile_data']), user_id))
    updated_rows = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    if updated_rows == 0:
        return jsonify({'error': 'Profile not found'}), 404
    return jsonify({'success': True}), 200

@app.route('/profiles/<int:user_id>', methods=['DELETE'])
def delete_profile(user_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('DELETE FROM profiles WHERE user_id = %s RETURNING *;', (user_id,))
    deleted_rows = cur.rowcount
    conn.commit()
    cur.close()
    conn.close()
    if deleted_rows == 0:
        return jsonify({'error': 'Profile not found'}), 404
    return jsonify({'success': True}), 204

if __name__ == '__main__':
    app.run(debug=True)
