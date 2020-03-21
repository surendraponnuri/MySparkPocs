//val days= Array ("MOn","Tues","wed","Thu","Fruday","Saturday","Sunday")
def convertDaysToDDDFormat(days:Array[String]) = for(t<- days) yield t.toLowerCase() match {
case "mon" | "monday" => "Mon"
     case "tue" | "tuesdaay" => "Tue"
     case "wed" | "wednesday" => "Wed"
     case "thu" | "thursday" => "Thu"
     case "fri" | "friday" => "Fri"
     case "sat" | "saturday" => "Sat"
     case "sun" | "sunday" => "Sun"
     case _ => t
     }

//recursive functions.. return datatype mandatory
//a function called itself ...
//factorial
6* 5*4*3*2*1
// x is safeguard
def fact(num:Int):Int= num match {
     case num if(num<=1)=> 1
     case _ => num * fact(num-1)
    //6* fact(5)
    //6* 5* 4 * 3* 2* 1
}
//to process a range value use "if guard"
fact(6)
val sal = 1000000
def tax(sal:Int) = sal match {
     case sal if(sal>1 && sal<500000) => "no tax"
     case sal if(sal >=500000 && sal<=750000) => "10% tax"
}

def add(a:Int, b:Int) = a+b
def mul(x:Int, y:Int) = x*y
def sqr(x:Int) =x*x
def cube(x:Int) = x*x*x
// nested function means a function call in another function, here no need return type.

def nestfunc(x:Int, y:Int) = { // x, y called parameter
     mul(x,y) + add(x,y)
}
nestfunc(5,4)
//higher order functions: a function call as parameter
def hof(x:Int, f:Int=>Int) = { // here f:Int=> Int, its a functin, it represent
     // take one Int and return int value
     f(x)
}

hof(8,sqr)
hof(7,cube)
def hof1(x:Int, y:Int, f:(Int, Int)=>Int) = {
     f(x,y)
}

hof1(9,15,mul)
//=> means do something action
// Anonymous Functions in Scala
(x:Int, y:Int)=> x+y
val names = Array("venu modi","satya modi","suri reddy","shika reddy")
names.map(x=>x.toUpperCase)
names.map(x=>x.contains(" "))
//scala ... where i want to use this
//flatmap = map + flatter
//apply a logig on top of each element next flatter result
names.flatMap(x=>x.split(" "))
