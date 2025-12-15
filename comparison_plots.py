import glob
import os
import sys
import json

import matplotlib.pyplot as plt


def plot_total_network_traffic(json_data: dict[str, list[tuple[int, dict]]]) -> None:
    for test_name, test_data in json_data.items():
        x = [0]
        y = [{"optimal": 0, "payload": 0, "control": 0}]

        for total_minutes, data in test_data:
            num_nodes = len(data["bytes_payload"].keys())
            x.append(total_minutes)
            y.append({
                "optimal": (num_nodes - 1) * total_minutes * 5 * 1024,
                "payload": sum(data["bytes_payload"].values()),
                "control": sum(data["bytes_control"].values()),
            })
        
        x_new = []
        y_new = []
        
        for i in range(1, len(y)):
            delta_payload = y[i]["payload"] - y[i-1]["payload"]
            delta_optimal = y[i]["optimal"] - y[i-1]["optimal"]
            
            if (delta_optimal > 0) and (delta_payload > 0):
                x_new.append(x[i])
                y_new.append(delta_payload / delta_optimal)
                
        
        plt.plot(x_new, y_new, label=test_name)
        
    plt.xlabel("Time (minutes)")
    plt.ylabel("Traffic multiple")
    plt.title("Multiple of Optimal Network Traffic")

    plt.legend()
    plt.savefig("network_traffic.png")
    plt.clf()


def generate_plots(dirs: dict[str, str]) -> None:
    json_data = {}

    for test_name, test_dir in dirs.items():
        json_data[test_name] = {}
        
        for json_path in glob.glob(os.path.join(test_dir, "data", "*.json")):
            file_name = os.path.basename(json_path)

            hours = int(file_name.split(":")[0])
            minutes = int(file_name.split(":")[1])
            total_minutes = 60 * hours + minutes

            with open(json_path, "r") as f:
                json_data[test_name][total_minutes] = json.load(f)
                
        json_data[test_name] = sorted(json_data[test_name].items())

    plot_total_network_traffic(json_data)


def main():
    generate_plots({
        "Gossipsub": sys.argv[1], 
        "DOG": sys.argv[2]
    })

        
if __name__ == "__main__":
    main()
