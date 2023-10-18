# HW2

Within this repository, you'll find a Docker Compose configuration tailored to establish a PostgreSQL environment and facilitate seamless data appending. The enduring data resulting from this configuration is stored within the Docker volumes under the <Folder Name>-db-data section.

## Running Instructions

To initiate the Docker Compose setup, adhere to the following steps:

1. **Clone the Repository:**

    ```bash
    git clone <repository-url>  # Replace <repository-url> with the actual URL of this GitHub repo
    cd <repository-directory>   # Replace <repository-directory> with the actual directory of the repo
    ```

2. **Run the Docker Compose File:**

    ```bash
    docker-compose up --build
    ```

    This command triggers the establishment of the PostgreSQL environment and oversees data operations as specified in the Compose file.


