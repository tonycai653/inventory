import datetime
import os
import shutil
import re
from collections import namedtuple
import csv


today_date = datetime.date.today()
delta = datetime.timedelta(days=1)
yesterday_date = today_date - delta


def get_records(logfile, pattern, exclude, list_records=None):
    ''' return list of sorted record from the logs
    record: (time, interface, message_code, job_id, process_path, message)
    '''
    
    pa = re.compile(pattern)
    if list_records is None:
        list_records = []
    with open(logfile, 'rt') as fr:
        for line in fr:
            m = pa.search(line)
            if m:
                delimit = m.group(1)
                if delimit in exclude:
                    continue
                Record = namedtuple('Record', ['log_time', 'interface', 'message_code', 
                                               'job_id', 'process_path', 'message'])

                lt = line.split(delimit)
                right_part = lt[1].split(':', 1)
                time = lt[0].split()[3].rsplit(':', 1)[0]
                interface = lt[0].rsplit(maxsplit=2)[0]
                second_part = right_part[1]
                first_part = right_part[0]
##                print('first_part: ', first_part)
##                print('second_part: ', second_part)
                record = Record(time, interface, *first_part.split(maxsplit=2),
                                message=second_part)
                list_records.append(record)
    return sorted(list_records, key=lambda l: l.job_id)


def get_date(line):
        months = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5,
              'Jun': 6, 'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10,
              'Nov': 11, 'Dec': 12}
        
        splits = line.split()
        if len(splits) >= 3:
            year, month, day, *_ = splits
        else:
            return None
        if year.isdigit() and month in months:
            year = int(year)
            month = months[month]
            day = int(day)
            return datetime.date(year, month, day)
        

def truncate(logfile, start_date=yesterday_date, end_date=today_date):
    '''get log content's date between start_date and end_date,
       end_date not included. [start_date, end_date)'''
    tempfile = 'tmp_' + str(os.getpid())
    with open(logfile, 'rt') as fr, open(tempfile, 'wt') as fw:
        sf = False
        for line in fr:           
                line_date = get_date(line)
                if line_date is None:
                    if sf  == True:
                        fw.writelines(line) #write this line to temp file
                    continue
                elif line_date < start_date:
                    continue
                elif line_date == start_date:
                    sf = True
                    fw.writelines(line)
                elif line_date == end_date:
                    break
                else:
                    fw.writelines(line)  
    shutil.move(tempfile, logfile)


def get_status(named_tuple):
    '''named_tuple - tuple with records with same job id'''
    for record_with_same_id in named_tuple:
        if 'exception' in record_with_same_id.message_code.lower():
            return 'EXCEPTION'
    return 'SUCCESS'
    


def get_category(record):
    '''get category like TrunkGroup, Switch, Carrier'''
    entry_pattern = r'Entry.?(\w+)'
    m = re.search(entry_pattern, record.message_code)
    if m:
        return m.group(1)


def get_identity(record):
    '''identity - customer id or trunkGroupId or trunkGroupClassId
    or swithId, etc'''
    message_patterns = [r'Input received for  with carrier ID -(\w*)',
                       r'Input received for Switch Transaction with carrier ID -(\w*)',
                       r'Input received for Trunk Group category Id -(\w*)',
                       r'Input received for Trunk Group  with Trunk Group ID -(\w*)',
                       r'Input received for  Trunk Group Class Id -(\w*)'
                       r'Process started for Carrier New Record with CustID: (\w*)',
                       r'Process started for Carrier Update Record with CustID: (\w*)',
                       r'Process started for Switch New Record with SwitchID: (\w*)',
                       r'Process started for Switch Update Record with SwitchD: (\w*)',
                       r'Process started for Trunk Group New Record with TrunkGroupID: (\w*)',
                       r'Process started for Trunk Group Update Record with TrunkGroupID: (\w*)',
                       r'Process started for Trunk Group Class New Record with TrunkClassID: (\w*)',
                       r'Process started for Trunk Group Class Update Record with TrunkClassID: (\w*)',
                       r'Process started for IME Hourly Export for TrunkGroupID: (\w*)',
                       r'Process started for IME Hourly Export for TrunkGroupID: (\w*)']
    for m_pa in message_patterns:
        m = re.search(m_pa, record.message.strip())
        if m:
            return m.group(1)
    

    
def process(logfile, outputfile='Invent Monitoring.csv'):
    pattern = r'(\[BW[-_]\w+\])'
    exclude = ['[BW-Core]', '[BW_Core]', '[BW_Plugin]']
    truncate(logfile)
    list_sorted_records = get_records(logfile, pattern, exclude)

# get list of list of records. The inner records has the same job id
    entities = []
    prev = -1
    current = -1
    for record in list_sorted_records:
        if record.job_id == prev:
            entities[current].append(record)
        else:
            current += 1
            entities.append([record,])
            prev = record.job_id
# write needed info to csv file
    with open(outputfile, 'at') as fw:
        csvw = csv.writer(fw, lineterminator='\n')
        
        for records_with_same_id in entities:
            for record in records_with_same_id:
                if 'Entry' in record.message_code:
                    csvw.writerow([yesterday_date, '', record.interface, get_category(record),
                                   get_identity(record), '', get_status(records_with_same_id),
                                   record.log_time, record.job_id])
    


    
if __name__ == '__main__':
      directory = 'log'
      for file in os.listdir(directory):
          process(os.path.join(directory, file))
##          print(file)
##          process(file)
        
