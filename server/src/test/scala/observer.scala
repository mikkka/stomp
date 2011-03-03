class Button(val name: String) {
  def click() = println("clicked")
}

abstract class SubjectObserver {
  type S <: Subject
  type O <: Observer

  trait Subject {
    self: S =>
    
    private var observers = List[O]()
    def addObserver(observer: O) = observers ::= observer
    def notifyObservers = observers foreach (_.receiveUpdate(self))
  }

  trait Observer {
    def receiveUpdate(subject: S)
  }
}

object ButtonSubjectObserver extends SubjectObserver {
  type S = ObservableButton
  type O = ButtonObserver

  class ObservableButton(name: String) extends Button(name) with Subject {
    override def click() = {
      super.click()
      notifyObservers
    }
  }

  trait ButtonObserver extends Observer {
    def receiveUpdate(button: ObservableButton)
  }
}

import ButtonSubjectObserver._

val obutton = new ObservableButton("butt")
val obs = new ButtonObserver {
  override def receiveUpdate(button: ObservableButton) = {
    println("click on " + button.name + " observerd")
  }
}
obutton.addObserver(obs)
obutton.click

