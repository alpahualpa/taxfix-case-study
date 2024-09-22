# Taxfix Case Study

This project is created as a result of the Taxfix Case solving.

# How to build and run

1. You need Docker to be installed. You can find a suitable version [[here]](https://docs.docker.com/get-started/get-docker/)

2. Clone repo to your local machine

3. Go into the cloned folder:

```bash
cd .\taxfix-case-study\
```

4. Build a docker image based on the cloned folder:

```bash
docker build -t taxfix-case-run .
```

5. Run the command and check the execution report in the Terminal window:

```bash
docker run taxfix-case-run
```
