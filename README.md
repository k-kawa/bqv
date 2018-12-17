# bqv

The simplest tool to manage Bigquery's views in CI/CD.

# How to install

```sh
go intall github.com/k-kawa/bqv
```

# How to use

Make directory based on the names of the dataset and the view that you want to create.

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

Make the views listed in the GCP project named `your_project` with `bqv apply` command.

```sh
bqv apply --projecID=your_project
INFO[0001] Creating view(your_dataset.your_view)
bra bra bra ....
```

## With parameter

You can use the `query.sql` file as a template and replace the placeholders written in it when you run `bqv apply`.

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

