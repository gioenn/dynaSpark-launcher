import json
import glob
import functools

def main():
    for json_file in glob.glob("./output_json/*.json"):
        with open(json_file) as json_data, open("./weight_json/" +json_file.split('/')[-1], "w") as jsonoutput:
            data = json.load(json_data)
            not_skipped = {k: v for k, v in data.items() if v['skipped'] == False }
            dataset = sorted(map(lambda args: (args[1].update({'id': int(args[0])}) or args[1]), not_skipped.items()), key=lambda v: v['id'], reverse=False)
            result = {}
            total_duration = sum(map(lambda x: x['duration'], dataset))/1000
            total_data = sum(map(lambda x: max(x['recordsread'], x['recordswrite'], x['shufflerecordsread'], x['shufflerecordswrite']), dataset))
            for i in range(0,len(dataset)):
                w1 = float(len(dataset)-i)
                w2 = sum(map(lambda x: x['duration'], dataset[i:]))/dataset[i]['duration']
                max_data = max(dataset[i]['recordsread'], dataset[i]['recordswrite'], dataset[i]['shufflerecordsread'], dataset[i]['shufflerecordswrite'])
                data_weight=sum(map(lambda x: max(x['recordsread'], x['recordswrite'], x['shufflerecordsread'], x['shufflerecordswrite']),dataset[i:]))/max_data
                w = w1*1/3 + w2*2/3
                dt = dataset[i]['duration']/1000
                nr = dataset[i]['nominalrate']
                time_spent = sum(map(lambda x: x['duration'], dataset[:i]))/1000
                local_deadline = (total_duration - time_spent)/w1
                input_record = nr * dt
                core = 80*input_record / (local_deadline * nr)
                result[dataset[i]['id']] = {'w1': w1, 'w2': w2, 'wt': w, 'dt' : dt, 'cs' : core, 'dw_weight': data_weight, 'data' : max_data, 'duration' : dataset[i]['duration']}
            json.dump(result, jsonoutput, indent=4)
if __name__ == "__main__":
    main()
