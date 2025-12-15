source .parse-env/bin/activate

for dir in 1000-*.data; do
    python3 parse_logs.py $dir
done

deactivate