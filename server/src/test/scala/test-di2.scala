// =======================
// service interfaces
trait OnOffDeviceComponent {
  val onOff: OnOffDevice
  trait OnOffDevice {
    def on: Unit
    def off: Unit
  }
}
trait SensorDeviceComponent {
  val sensor: SensorDevice
  trait SensorDevice {
    def isCoffeePresent: Boolean
  }
}

// =======================
// service implementations
trait OnOffDeviceComponentImpl extends OnOffDeviceComponent {
  class Heater extends OnOffDevice {
    def on = println("heater.on")
    def off = println("heater.off")
  }
}
trait SensorDeviceComponentImpl extends SensorDeviceComponent {
  class PotSensor extends SensorDevice {
    def isCoffeePresent = true
  }
}
// =======================
// service declaring two dependencies that it wants injected
trait WarmerComponentImpl {
  this: SensorDeviceComponent with OnOffDeviceComponent =>
  class Warmer {
    def trigger = {
      if (sensor.isCoffeePresent) onOff.on
      else onOff.off
    }
  }
}

// =======================
// instantiate the services in a module
//class ComponentRegistry extends
object ComponentRegistry extends
  OnOffDeviceComponentImpl with
  SensorDeviceComponentImpl with
  WarmerComponentImpl {

  val onOff = new Heater
  val sensor = new PotSensor
  val warmer = new Warmer
}

// =======================
//val cr = new ComponentRegistry()
//val warmer = cr.warmer
val warmer = ComponentRegistry.warmer 
warmer.trigger
