package test

abstract class mylist {

  def head:Int
  def tail:mylist
  def isempty:Boolean
  def add(element:Int):mylist
  def printelement : String

  override def toString: String = "[" + printelement + "]" 


}

object empty extends mylist
{
  def head:Int = throw new NoSuchElementException;
  def tail:mylist = throw new NoSuchElementException;
  def isempty:Boolean = true;
  def add(element:Int):mylist = new cons(element,empty)

  override def printelement: String = ""


}

class cons(h:Int, t:mylist) extends mylist
{
  def head:Int = h
  def tail:mylist = t
  def isempty:Boolean = false;
  def add(element:Int):mylist = new cons(element,this)

  override def printelement: String =
    if (t.isempty) "" + h
    else h + " " + t.printelement
}

object listtest extends App
{
  val list =new   cons(1,new cons(2,new cons(3,empty)))
  println(list.tail)
  println(list.add(4).head)

  println(list.tail)
}