# bqv

The simplest tool to manage Bigquery's views.

<!-- TOC -->

- [bqv](#bqv)
- [How to install](#how-to-install)
- [How to use](#how-to-use)
    - [With parameter](#with-parameter)

<!-- /TOC -->

# How to install

```sh
go install github.com/k-kawa/bqv
```

Or you can use `bqv` as a Docker image, which is available at [kkawa/bqv](https://cloud.docker.com/repository/docker/kkawa/bqv)


# How to use

Make a directory based on the names of the dataset and the view that you want to create.

```sh
mkdir -p your_dataset/your_view
```

Make an `query.sql` file in it, which is going to use to create the view.

```sh
cat <<EOF > your_dataset/your_view/query.sql
SELECT 1 AS one
EOF
```

List the view names which are going to manage with `bqv list` command.

```sh
bqv list
your_dataset.your_view
```

Make the view into the GCP project named `your_project` with `bqv apply` command.

```sh
bqv apply --projecID=your_project
INFO[0001] Creating view(your_dataset.your_view)
bra bra bra ....
```

Destroy all the views in the GCP project with `bqv destroy` command.

```sh
bqv destroy --projectID=your_project
INFO[0001] Deleting view your_dataset.your_view
```

## With parameter

You can use the `query.sql` file as a template and replace the placeholders in it when you run `bqv apply`.

```sh
# Create another directory
mkdir -p your_dataset/your_new_view

# Create the new query.sql following the Golang's template syntax.
cat <<EOF > your_dataset/your_new_view/query.sql
SELECT "{{.data}}" AS data
EOF

# Prepare a JSON file the keys and values in which are going to fill the query.sql
cat <<EOF > parameters.json
{"data": "data"}
EOF

# Run bqv apply with the parameters.json
bqv apply --paramFile=parameters.json
```

