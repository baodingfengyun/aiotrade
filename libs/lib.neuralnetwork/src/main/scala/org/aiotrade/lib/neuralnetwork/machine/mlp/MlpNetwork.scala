/*
 * Copyright (c) 2006-2011, AIOTrade Computing Co. and Contributors
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

package org.aiotrade.lib.neuralnetwork.machine.mlp

import java.util.logging.Logger
import org.aiotrade.lib.neuralnetwork.core.NetworkChangeEvent
import org.aiotrade.lib.neuralnetwork.core.descriptor.NetworkDescriptor
import org.aiotrade.lib.neuralnetwork.core.model.AbstractNetwork
import org.aiotrade.lib.collection.ArrayList
import org.aiotrade.lib.math.vector.InputOutputPointSet
import org.aiotrade.lib.math.vector.Vec
import org.aiotrade.lib.util.Argument

/**
 * Multi-Layer Perceptron network.
 *
 * @author Caoyuan Deng
 */
class MlpNetwork extends AbstractNetwork {
  private val log = Logger.getLogger(getClass.getName)

  val neuralNetworkName= "Multi-Layer Perceptron"

  private var _descriptor: MlpNetworkDescriptor = _
  private var _layers = new ArrayList[MlpLayer]()
  private var _arg: MlpNetwork.Arg = _
    
  @throws(classOf[Exception])
  def init(descriptor: NetworkDescriptor) {
    _descriptor = descriptor.asInstanceOf[MlpNetworkDescriptor]
        
    /** setup the network layers */
        
    _layers.clear
        
    val firstLayerDescriptor = _descriptor.layerDescriptors(0)
    val firstLayer = new MlpHiddenLayer(
      null,
      descriptor.dataSource.inputDimension,
      firstLayerDescriptor.numNeurons,
      firstLayerDescriptor.neuronClassName)
        
    _layers += firstLayer
        
    val n = descriptor.numLayers
    var i = 1
    while (i < n) {
      val currLayerDescriptor = descriptor.layerDescriptors(i)
            
      val currLayer = if (i == n - 1) {
        /** output layer */
        new MlpOutputLayer(
          _layers(i - 1).numNeurons,
          currLayerDescriptor.numNeurons,
          currLayerDescriptor.neuronClassName)
                
      } else {
        /** hidden layer */
        new MlpHiddenLayer(
          null,
          _layers(i - 1).numNeurons,
          currLayerDescriptor.numNeurons,
          currLayerDescriptor.neuronClassName)
                
      }
            
      _layers += currLayer
            
      val backLayer = _layers(i - 1)
      backLayer.connectTo(currLayer)
      
      i += 1
    }
        
    _arg = descriptor.arg.asInstanceOf[MlpNetwork.Arg]
  }

  def arg = _arg
  def arg_=(arg: MlpNetwork.Arg) {
    _arg = arg
  }
    
  def layers = _layers
  def layers_=(layers: ArrayList[MlpLayer]) {
    _layers = layers
  }
    
  def removeLayer(idx: Int) {
    _layers.remove(idx)
  }
        
  def inputDimension  = _layers.head.inputDimension
  def outputDimension = _layers.last.numNeurons
    
  def numLayers = _layers.length
    
  def predict(input: Vec): Vec = {
    propagate(input)
        
    output
  }
    
  protected def output: Vec = {
    val outputLayer = _layers.last
        
    outputLayer.neuronsOutput
  }
    
  protected def propagate(input: Vec) {
    val firstLayer = _layers.head
        
    firstLayer.setInputToNeurons(input)
    
    val n = _layers.length - 1
    var i = 0
    while (i < n) {
      _layers(i).propagateToNextLayer
      i += 1
    }
  }
    
  /**
   * Backpropagate layer to layer
   * Compute delta etc and do adapt
   */
  protected def backPropagate(expectedOutput: Vec) {
    val outputLayer = _layers.last.asInstanceOf[MlpOutputLayer]
        
    outputLayer.setExpectedOutputToNeurons(expectedOutput)
    
    var i = _layers.length - 1
    while (i >= 0) {
      _layers(i).backPropagateFromNextLayerOrExpectedOutput
      i -= 1
    }
        
  }
    
  protected def propagteBidirection(input: Vec, expectedOutput: Vec): Double = {
    /**
     * @Note:
     * Should do propagate() first, then delta of layers, because propagate()
     * will reset the new input of network, and propagate in the network.
     * Delta should be computed after that.
     *
     * Be ware of the computing order: compute the delta backward. then adapt
     */
        
    /** 1. forward propagate */
    propagate(input)
        
    /** 2. get network output, this should be done before back-propagate */
    val prediction = output
        
    /** 3. Compute error */
    val error = prediction.metric(expectedOutput) / prediction.dimension
        
    /** 4. back-propagate, this will compute delta etc and do adapt, ie, do  */
    backPropagate(expectedOutput)
        
    /** 5. return error */
    error
  }
    
  def train(iops: InputOutputPointSet) {
    //trainSerialMode(iops)
    trainBatchMode(iops)
  }
    
  private def trainSerialMode(iops: InputOutputPointSet) {
    var break = false
    var epoch = 1L
    while (epoch <= arg.maxEpoch && !break) {
            
      /** re-randomize iops order each time */
      iops.randomizeOrder
            
      var epochSumError = 0.0
      var i = 0
      while (i < iops.size) {
        epochSumError += propagteBidirection(iops(i).input, iops(i).output)
        adapt()
        i += 1
      }
            
      val epochMeanError = epochSumError / iops.size
            
      fireNetworkChangeEvent(new NetworkChangeEvent(
          this,
          NetworkChangeEvent.Type.Updated,
          epoch,
          epochMeanError
        ))
            
      //println("Mean Error at the end of epoch " + epoch + ": " + epochMeanError)
            
      if (epochMeanError < arg.predictionError) {
        break = true
      } else {
        epoch += 1
      }
    }
  }
    
  private def trainBatchMode(iops: InputOutputPointSet) {
    var break = false
    var epoch = 1L
    while (epoch <= arg.maxEpoch && !break) {
      var epochSumError = 0.0
      var i = 0
      while (i < iops.size) {
        epochSumError += propagteBidirection(iops(i).input, iops(i).output)
        i += 1
      }
      adapt()
            
      val epochMeanError = epochSumError / iops.size
            
      fireNetworkChangeEvent(new NetworkChangeEvent(
          this,
          NetworkChangeEvent.Type.Updated,
          epoch,
          epochMeanError
        ))
            
      //println("Mean Error at the end of epoch " + epoch + ": " + epochMeanError);
            
      if (epochMeanError < arg.predictionError) {
        break = true
      } else {
        epoch += 1
      }
    }
  }
    
    
  def learnOnePoint(input: Vec, expectedOutput: Vec): Double = {
    val error = propagteBidirection(input, expectedOutput)
        
    adapt()
        
    error
  }
    
  private def adapt() {
    _layers foreach (_.adapt(arg.learningRate, arg.momentumRate))
  }
    
  def cloneDescriptor(): NetworkDescriptor = {
    /** @TODO */
    _descriptor
  }
}

object MlpNetwork {
  case class Arg(
    maxEpoch: Long,
    learningRate: Double,
    momentumRate: Double,
    predictionError: Double
  ) extends Argument  
}