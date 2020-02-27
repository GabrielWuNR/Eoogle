import threading
import SearchHandle
import time

start = time.time()
searchservice = SearchHandle.SearchHandle()
mid1 = time.time()
def testThread(threadname, searchservice):
    start = time.time()
    searchservice = SearchHandle.SearchHandle()
    mid1 = time.time()
    print("the init time is :", mid1 - start)

    connector1 = SearchHandle.SqlCreator()
    connector2 = SearchHandle.SqlCreator()

    start2 = time.time()
    put = searchservice.initTerm("wait", connector1.getConn())
    get = searchservice.initTerm("jame", connector2.getConn())
    mid2 = time.time()
    print("time of finding data from db:", mid2 - start2)
    # distance search
    start3 = time.time()
    Dis_searchresult = searchservice.getNewDisResult(put, get, 100)
    mid3 = time.time()
    print("the dis search time is: ", mid3 - start3)
    # And search()
    start4 = time.time()
    and_searchresult = searchservice.getNewAndResult(put, get)
    mid4 = time.time()
    print("the AND search time is: ", mid4 - start4)
    # Or search()
    start5 = time.time()
    or_searchresult = searchservice.getNewOrResult(put, get)
    mid5 = time.time()
    print("the or search time is: ", mid5 - start5)
    # Xor search()
    start6 = time.time()
    xor_searchresult = searchservice.getNewXorResult(put, get)
    mid6 = time.time()
    print("the xor search time is: ", mid6 - start6)
    # Nei search()
    start7 = time.time()
    nei_searchresult = searchservice.getNewNeiResult(put, get)
    mid7 = time.time()
    print("the nei search time is: ", mid7 - start7)

    # full exmaple:
    start8 = time.time()
    put = searchservice.initTerm("wait", connector1.getConn())
    get = searchservice.initTerm("jame", connector2.getConn())
    example_or_search = searchservice.getNewOrResult(put, get)
    example_searchresult = searchservice.newFinalize(example_or_search, connector1.getConn())
    print("the example search time is ", time.time() - start8)
    # print(example_searchresult)  
  


t1 = threading.Thread(target=testThread, args=(1, searchservice))
t2 = threading.Thread(target=testThread, args=(2,searchservice))
t1.start()
t2.start()
t1.join()
t2.join()

