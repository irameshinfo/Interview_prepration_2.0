def string_rev():
    try:
        val=input("please enter the value \n")
        result=val[::-1]
        print(f"resversed value is \n {result}")
        count=len(val)
        print(f"length of {val} is {count}")
        num=int(input("enter number"))
        if num%2==0:
            print("even")
        else:
            print("odd")    
    except Exception as e:  
        print(f"exception is {e}")  
string_rev()    