/*
 * Copyright (c) 2006-2007, AIOTrade Computing Co. and Contributors
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without 
 * modification, are permitted provided that the following conditions are met:
 * 
 *  o Redistributions of source code must retain the above copyright notice, 
 *    this list of conditions and the following disclaimer. 
 *    
 *  o Redistributions in binary form must reproduce the above copyright notice, 
 *    this list of conditions and the following disclaimer in the documentation 
 *    and/or other materials provided with the distribution. 
 *    
 *  o Neither the name of AIOTrade Computing Co. nor the names of 
 *    its contributors may be used to endorse or promote products derived 
 *    from this software without specific prior written permission. 
 *    
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" 
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, 
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR 
 * PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR 
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; 
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR 
 * OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, 
 * EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.aiotrade.modules.ui.actions;
import org.aiotrade.lib.charting.view.WithQuoteChart;
import org.aiotrade.lib.indicator.Indicator
import org.aiotrade.lib.math.indicator.IndicatorDescriptor
import org.aiotrade.lib.util.swing.action.EditAction;
import org.aiotrade.modules.ui.windows.AnalysisChartTopComponent;
import org.openide.util.HelpCtx;
import org.openide.util.NbBundle
import org.openide.util.actions.CallableSystemAction;

/**
 *
 * @author Caoyuan Deng
 */
class ChangeOptsAction extends CallableSystemAction {
    
  /** Creates a new instance
   */
  def performAction {
    java.awt.EventQueue.invokeLater(new Runnable() {
        def run {
          val analysisWin = AnalysisChartTopComponent.selected getOrElse {return}
                
          val selectedView = analysisWin.viewContainer.selectedView
          if (selectedView == null) {
            return
          }

          var indicator: Indicator = if (selectedView.isInstanceOf[WithQuoteChart]) {
            val ind = selectedView.overlappingSers find (x => x.isInstanceOf[Indicator]) getOrElse null
            ind.asInstanceOf[Indicator]
          } else {
            selectedView.mainSer match {
              case ind: Indicator => ind
              case _ => null
            }
          }

          if (indicator == null) {
            return
          }
                
          val contents = analysisWin.viewContainer.controller.contents
          for (descriptor <- contents.lookupDescriptor(classOf[IndicatorDescriptor],
                                                       indicator.getClass.getName,
                                                       selectedView.mainSer.freq)
          ) {
            descriptor.lookupAction(classOf[EditAction]) foreach {_.execute}
          }
                
          
        }
      })
        
  }
    
  def getName: String = {
    //"Change Current Indicator's Options"
    val name = NbBundle.getMessage(this.getClass,"CTL_ChangeOptsAction")
    name
  }
    
  def getHelpCtx: HelpCtx = {
    HelpCtx.DEFAULT_HELP
  }
    
  override protected def iconResource: String = {
    "org/aiotrade/modules/ui/resources/changeOpts.gif"
  }
    
  override protected def asynchronous: Boolean = {
    false
  }
    
    
}

