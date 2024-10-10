import traceback as tb

visible = True
traceback = False



def dynamicPrinter(str):
    if visible == True:
        print(str)

    if traceback == True:
        if Exception != 'NoneType':
            tb.print_exc()
