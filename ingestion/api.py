from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/file-uploaded', methods=['POST'])
def file_uploaded():
    # Get the data from the request
    result = request.json  # Assuming the data is sent as JSON
    print(request.json)
    return jsonify(result), 200

if __name__ == '__main__':
    app.run(debug=True, port=5010)
