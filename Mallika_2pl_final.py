
#-----------------------------------------------------------

trans = []          #It is used to store all the transaction in list format
f = open("Transactions_4.txt")
#f = open("file.txt")
ts = {}             #it is used to store the timestamp of the transactions
maxoptr = {}        #it is used to store the value of max no. of operations each transaction has.
val = 10            #to assign timestamp
for lines in f.readlines():
    t = lines.strip('\n')
    k = t.split(':')
    ts[k[0]] = val
    val = val + 10
    trans.append(k)
    #print(k)
#print(trans)
#print(ts)
#To Check 2pl
allop = 0           #to store the total no. of operations in the file        
flagpl = 1
for list1 in trans:
    #print(list1)
    #print(list1[1])
    u = 0           #to check that unlocking is started or not.
    op = list1[1].split(',');
    #print(op)
    maxoptr[list1[0]] = len(op)
    allop = allop + len(op);
    #print(op)
    for i in range(0,len(op)):
        #print(op[i])
        if op[i][0] == 'u':
            u = 1
        elif op[i][0] == 'x' and u == 1:
            #It is already unlocked i.e why u=1 and after unlocking we cannot do locking
            flagpl = 0
#print(allop) = 21 is the total number of operations
#print(maxoptr)
if flagpl == 0:
    print("Not in 2pl")
    input("Press any key to exit.")
    exit()
print("The given schedule is in 2pl")
#trans=[['t1', 'xl(a),w(a),xl(c),w(c),xl(b),w(b),ul(a),ul(b),ul(c)'], ['t2', 'xl(b),r(b),w(b),xl(a),r(a),w(a),ul(b),ul(a)'], ['t3', 'xl(b),w(b),r(b),ul(b)']]

tptr = {}           #transaction pointer for storing how many instructions each transaction executed.
for i in range(0,len(trans)):
    tptr[trans[i][0]] = 0
#print(tptr)
#{'t3': 0, 't2': 0, 't1': 0}
#print(allop)
lockmgr = {}        #the lock manager to store lock varibale and corresponding transaction. 
ul = []             #list for recording first unlock
rb = []             #list for recording rollbacks

#----------Check for younger and older transaction----------

def chkts(ti,tj):
    if ts[ti]>ts[tj]:
        return False
    return True

#-----------------------------------------------------------

#----------To add a new lock in lockmgr---------------------

def addlock(lockvar,locktr):
    lockmgr[lockvar] = locktr

#-----------------------------------------------------------

#----------To delete lock in lockmgr------------------------
    
def dellock(lockvar):
    del lockmgr[lockvar]



#----------To rollback younger transaction------------------
#during rollback the transaction pointer of transaction which is being rollbacked is set to maximum value
#so that it seems as if the transaction is completed.

def rollback(tr):
    #tr= tid of the transaction rolledback
    tptr[tr] = maxoptr[tr]
    for de in lockmgr:
        if lockmgr[de] == tr:
            del lockmgr[de]
            lockmgr['$'] = '$'


            
#----------Execution of Instructions in Round Robin Fashion starts here----------

opcount = 0         #to count how many operations have been executed from the total operation in file.

print("The Serializability order is: ")
while opcount<allop:
    for i in range(0,len(trans)):
        op = trans[i][1].split(',') #op stores all the operation of current transaction in list form
        ti = trans[i][0]            #ti is the current transation
        if tptr[ti]<len(op):        #if the number of operations executed by the transaction is < total number of operations in the transaction
            for j in range(0,4):
                if tptr[ti]<len(op):
                    o = op[tptr[ti]]    #o stores the single operation like 'xl(a)'
                    if o[0] == 'x':
                        #print(o[0])
                        if o[3] not in lockmgr:     #if the data item is not already locked
                            addlock(o[3],ti)
                            tptr[ti] = tptr[ti] + 1
                        else:
                            #if the data item is in lockmanager then it may be locked by an older transaction
                            if(chkts(trans[i][0],lockmgr[o[3]])):
                                rb.append(lockmgr[o[3]]) #we will append the tid of the rolled back transaction
                                rollback(lockmgr[o[3]])
                    elif o[0] == 'u':
                        #print(o[0])
                        dellock(o[3])
                        if ti not in ul:
                            ul.append(ti)
                        tptr[ti] = tptr[ti] + 1
                    else:
                        #print(o[0])
                        #for read or write
                        tptr[ti] = tptr[ti] + 1
                    #print(tptr[ti])
                    #print(lockmgr)
    opcount = 0
    for ti in tptr:
        opcount = opcount + tptr[ti] 
    #print(opcount)
order = "S"
#print(ul)
#print(rb)
for ti in ul:
    if ti not in rb:
        order = order + " -> " + ti
print(order)

print("Rollback=")
print(rb)
