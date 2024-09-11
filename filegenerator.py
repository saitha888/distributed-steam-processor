import random
from faker import Faker
import sys


def generate_file(lines, filename):
    fake = Faker()

    error_types = ["INFO", "DEBUG", "WARN", "ERROR"]

    messages = ["System started",
	"User login successful",
	"Invalid password attempt",
	"Disk space running low",
	"Service restarted",
	"File not found",
	"Database connection failed",
	"Timeout occurred",
	"Invalid user input",
	"Memory allocation failure"]

    output = []

    for i in range(lines):
        error_index = random.randint(0, len(error_types) - 1)
        message_index = random.randint(0, len(messages) - 1)

        error = error_types[error_index]
        message = messages[message_index]
        server_name = "server-name"
        time = fake.date_time().strftime('%b %d %H:%M:%S')


        line = time + " " + server_name + " " + error + " " + message 
        output.append(line)

    with open(filename, 'w') as file:
        for item in output:
            file.write(f"{item}\n") 

def main():
    arguments = sys.argv[1:] 
    generate_file(int(arguments[0]), arguments[1])

main()

    


