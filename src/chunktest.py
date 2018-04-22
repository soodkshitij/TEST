import uuid
import datetime

def process(file_obj, request = True, name=""):
    if request:
        name = uuid.uuid4().hex
        file_obj.save(os.path.join("/tmp", name))
    with open(name) as f:
        process = False
        chunk_size = 500
        data = []
        for line in f:
            #print line.strip().split()
            if len(line.strip().split())>10 and (line.strip().split())[0] == "STN":
                process = True
                continue
            
            if process:
                tempLine = line.strip().split()
                tempLine[1] = datetime.datetime.strptime(tempLine[1], '%Y%m%d/%H%M').strftime('%Y-%m-%d %H:%M:%S')
                data.append(','.join(tempLine)+"\n")
                
            if len(data) ==chunk_size:
                print ("Yielding data ",len(data))
                yield(data)
                data =[]
    if data:
        print ("Yielding data ",len(data))
        yield(data)
        
