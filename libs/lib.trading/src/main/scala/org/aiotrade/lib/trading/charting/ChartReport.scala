package org.aiotrade.lib.trading.charting

import java.awt.BorderLayout
import java.awt.Color
import java.awt.Container
import java.awt.event.ActionListener
import java.awt.image.BufferedImage
import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date
import java.util.Random
import java.util.concurrent.CountDownLatch
import java.util.logging.Level
import java.util.logging.Logger
import javafx.application.Platform
import javafx.collections.FXCollections
import javafx.embed.swing.JFXPanel
import javafx.geometry.BoundingBox
import javafx.geometry.Bounds
import javafx.scene.Scene
import javafx.scene.chart.CategoryAxis
import javafx.scene.chart.LineChart
import javafx.scene.chart.NumberAxis
import javafx.scene.chart.XYChart
import javafx.scene.control.Tab
import javafx.scene.control.TabPane
import javafx.scene.layout.VBox
import javax.imageio.ImageIO
import javax.swing.JFrame
import javax.swing.JOptionPane
import javax.swing.Timer
import org.aiotrade.lib.trading.Param
import org.aiotrade.lib.trading.ReportData
import org.aiotrade.lib.util.actors.Reactor
import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.concurrent.SyncVar

/**
 * 
 * @author Caoyuan Deng
 */
class ChartReport(
  imageFileDirStr: String, isAutoRanging: Boolean = true, 
  upperBound: Int = 0, lowerBound: Int = 1000, 
  width: Int = 1200, height: Int = 900
) {
  private val log = Logger.getLogger(this.getClass.getName)
  
  private val cssUrl = Thread.currentThread.getContextClassLoader.getResource("chart.css").toExternalForm
  private val imageFileDir = {
    try {
      val dir = new File(if (imageFileDirStr != null) imageFileDirStr else ".")
      if (!dir.exists) {
        dir.mkdirs
      }
      dir
    } catch {
      case ex: Throwable => throw(ex)
    }
  }
  private val fileDf = new SimpleDateFormat("yyMMddHHmm")

  private var imageSavingLatch: CountDownLatch = _
  private var chartTabs = List[ChartTab]()
  
  private val frame = new JFrame()
  private val jfxPanel = new JFXPanel()
  private val tabPane = new TabPane()
  
  initAndShowGUI
  
  private def runInFXThread(block: => Unit) {
    Platform.runLater(new Runnable {
        def run = block // @Note don't write to: def run {block}
      })
  }
  
  private def initAndShowGUI {
    // should put this code outside of runInFXThread, otherwise will cause: Toolkit not initialized
    frame.add(jfxPanel, BorderLayout.CENTER)
    
    runInFXThread {
      val scene = new Scene(tabPane, width, height)
      scene.getStylesheets.add(cssUrl)
      jfxPanel.setScene(scene)

      frame.pack
      frame.setVisible(true)
    }
  }
  
  /**
   * @param for each param in params, will create a new tabbed pane in main frame.
   */
  def roundStarted(params: List[Param]) {
    if (imageSavingLatch != null) {
      try {
        imageSavingLatch.await
      } catch {
        case e: InterruptedException => e.printStackTrace
      }
    }
    chartTabs = params map (new ChartTab(_))
    Thread.sleep(1000) // wait for chartTab inited in FX thread
  }
  
  def roundFinished {
    imageSavingLatch = new CountDownLatch(chartTabs.length)
    Thread.sleep(2000) // wait for chart painted in FX thread
    chartTabs foreach (_.saveImage)
  }
  
  private class ChartTab(param: Param) extends Reactor {
    private val idToSeries = new mutable.HashMap[String, XYChart.Series[String, Number]]()
    private var valueChart: LineChart[String, Number] = _
    private var referChart: LineChart[String, Number] = _

    private val df = new SimpleDateFormat("yy.MM.dd")

    private val tab = new Tab()
    private var root: VBox = _
    
    initAndShowGUI
  
    reactions += {
      case (data: ReportData, color: Color) => updateData(data, color)
      case data: ReportData => updateData(data, null)
    }
    listenTo(param)
  
    private def initAndShowGUI {
      runInFXThread {
        root = new VBox()

        val xAxis = new CategoryAxis()
        xAxis.setLabel("Time")
        val yAxis = new NumberAxis()
        yAxis.setAutoRanging(isAutoRanging)
        yAxis.setUpperBound(upperBound)
        yAxis.setLowerBound(lowerBound)
      
        valueChart = new LineChart[String, Number](xAxis, yAxis)
        valueChart.setTitle("Equity Monitoring - " + param.titleDescription)
        valueChart.setCreateSymbols(false)
        valueChart.setLegendVisible(false)
        valueChart.setPrefHeight(0.9 * height)

        val xAxisRef = new CategoryAxis()
        xAxisRef.setLabel("Time")
        val yAxisRef = new NumberAxis()
      
        referChart = new LineChart[String, Number](xAxisRef, yAxisRef)
        referChart.setCreateSymbols(false)
        referChart.setLegendVisible(false)
      
        root.getChildren.add(valueChart)
        root.getChildren.add(referChart)
        tab.setContent(root)
        tab.setText(param.titleDescription)

        tabPane.getTabs.add(tab)
      }
    }
  
    private def resetData {
      runInFXThread {
        idToSeries.clear
        valueChart.setData(FXCollections.observableArrayList[XYChart.Series[String, Number]]())
        referChart.setData(FXCollections.observableArrayList[XYChart.Series[String, Number]]())
      }
    }
  
    private def updateData(data: ReportData, color: Color) {
      // should run in FX application thread
      runInFXThread {
        val serieId = data.name + data.id
        val serieName = data.name + "-" + data.id
        val chart = if (data.name.contains("Refer")) referChart else valueChart
        val series = idToSeries.getOrElseUpdate(serieId, createSeries(serieName, chart))
        series.getData.add(new XYChart.Data(df.format(new Date(data.time)), data.value))

        val styleSelector = "series-" + serieId
        if (!series.getNode.getStyleClass.contains(styleSelector)) {
          series.getNode.getStyleClass.add(styleSelector)
        }
        
        if (color != null) {
          val nodes = chart.lookupAll("." + styleSelector)
          nodes foreach (_.setStyle("-fx-stroke: #" + toHexColor(color) +  "; "))
        }
      }
    }
    
    /**
     * Should mask first 2 digits of "XX"-contribution from the Alpha-component (which is not always the case)
     */
    private def toHexColor(color: Color) = Integer.toHexString((color.getRGB & 0xffffff) | 0x1000000).substring(1)
    
    private def createSeries(name: String, chart: XYChart[String, Number]): XYChart.Series[String, Number] = {
      val series = new XYChart.Series[String, Number]()
      series.setName(name)
      chart.getData.add(series)
      series
    }
  
    /**
     * @return SyncVar[ChartTab](this)
     */
    def saveImage {
      val done = new SyncVar[ChartTab]()
      val file = new File(imageFileDir, fileDf.format(new Date(System.currentTimeMillis)) + "_" + param.shortDescription + ".png")
      tabPane.getSelectionModel.select(tab)
      
      // wait for sometime after select this tab via timer
      var timer: Timer = null
      timer = new Timer(1500, new ActionListener {
          def actionPerformed(e: java.awt.event.ActionEvent) {
            ChartReport.saveImage(jfxPanel, file)
            tabPane.getTabs.remove(tab)
            done.put(ChartTab.this)
            imageSavingLatch.countDown
            timer.stop
          }
        }
      )
      timer.start
      
      done.take
    }
  }
  
}


object ChartReport {
  private val log = Logger.getLogger(this.getClass.getName)
  
  private def saveImage(container: Container, file: File)  {
    try {
      val name = file.getName
      val ext = name.lastIndexOf(".") match {
        case dot if dot >= 0 => name.substring(dot + 1)
        case _ => "jpg"
      }
      val boundbox = new BoundingBox(0, 0, container.getWidth, container.getHeight)
      ImageIO.write(toBufferedImage(container, boundbox), ext, file)
      println("=== Image saved ===")
    } catch {
      case ex: Throwable =>
        log.log(Level.WARNING, ex.getMessage, ex)
        JOptionPane.showMessageDialog(null, "The image couldn't be saved", "Error", JOptionPane.ERROR_MESSAGE)
    }
  }
  
  /**
   * This function is used to get the BufferedImage of the container as JFXPanel etc
   * @param container
   * @param bounds
   * @return
   */
  private def toBufferedImage(container: Container, bounds: Bounds): BufferedImage = {
    val bufferedImage = new BufferedImage(bounds.getWidth.toInt,
                                          bounds.getHeight.toInt,
                                          BufferedImage.TYPE_INT_ARGB)

    val g = bufferedImage.getGraphics
    g.translate(-bounds.getMinX.toInt, -bounds.getMinY.toInt) // translating to upper-left corner
    container.paint(g)
    g.dispose
    bufferedImage
  }
  

  // -- simple test
  def main(args: Array[String]) {
    val cal = Calendar.getInstance
    // should hold chartReport instance, otherwise it may be GCed and cannot receive message. 
    val chartReport = new ChartReport(".")
    val params = List(TestParam(1), TestParam(2))
    chartReport.roundStarted(params)
    
    val random = new Random(System.currentTimeMillis)
    cal.add(Calendar.DAY_OF_YEAR, -10)
    for (i <- 1 to 10) {
      cal.add(Calendar.DAY_OF_YEAR, i)
      params foreach {_.publish((ReportData("series", 0, cal.getTimeInMillis, random.nextDouble), Color.CYAN))}
    }
    
    chartReport.roundFinished
  }
  
  private case class TestParam(v: Int) extends Param
}
