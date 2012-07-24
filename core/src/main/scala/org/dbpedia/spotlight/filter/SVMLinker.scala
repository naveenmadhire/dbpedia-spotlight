package org.dbpedia.spotlight.filter

import weka.core._
import weka.core.converters.{CSVLoader}
import java.io._
import org.dbpedia.spotlight.exceptions.{ConfigurationException}
import weka.classifiers.Classifier

import org.apache.commons.logging.LogFactory
import org.dbpedia.spotlight.model.DBpediaResource

/**
 *
 * @author pablomendes
 */

class SVMLinker(val modelFile: File) {

    private val LOG = LogFactory.getLog(this.getClass)

    val POSITIVE = "yes"
    val NEGATIVE = "no"

    lazy val classifier = {
        LOG.info("Loading model from " + modelFile.getAbsolutePath)
        if (modelFile.exists()) {
            new ObjectInputStream(new FileInputStream(modelFile)).readObject().asInstanceOf[Classifier]
        } else {
            throw new ConfigurationException("Could not load model file %s.".format(modelFile))
        }
    }

    def getInstance(features: Map[String, String]): Instances = {
        val header = features.keys.toSeq.filterNot(_.equals("class")).sorted.map("\"" + _ + "\"").mkString(",") + "\n"
        val values = features.keys.toSeq.filterNot(_.equals("class")).sorted.map("\"" + features(_) + "\"").mkString(",") + "\n"
        val csv = header + values
        val loader = new CSVLoader
        loader.setSource(new ByteArrayInputStream(csv.getBytes("UTF-8")))
        val data = loader.getDataSet
        val fvClassVal = new FastVector[String](2)
        //        fvClassVal.addElement(NEGATIVE)
        fvClassVal.addElement(POSITIVE)
        val label: Attribute = new Attribute("class", fvClassVal)
        data.insertAttributeAt(label, data.numAttributes())
        if (data.classIndex() == -1)
            data.setClassIndex(data.numAttributes() - 1)
        data
    }

    def classify(tag: (DBpediaResource, Map[String, String])) = {
        val features = tag._2
        val data = getInstance(features)
        val unlabeled = data.instance(0)
        val clsLabel = classifier.classifyInstance(unlabeled);
        println("Decision: %s // Instance: %s".format(clsLabel.toString, unlabeled))
        //        if (clsLabel==0.0)
        //            println(tag._1.uri)
        clsLabel > 0.0
    }

}

object SVMLinker {

    def main(args: Array[String]) {
        val modelFile = new File("/home/pablo/eval/bbc/svm-test1.model")

        val linker = new SVMLinker(modelFile)
        val svm = linker.classifier

        val labels = List("class", "contextual", "organisation", "person", "place", "prior", "theme", "topical")
        //val values = List("yes","1.0","0.0","0.0","0.0","1.055E-5","0.0","1.052688368714628E-4")
        val values = List("yes", "0.000429", "1", "0", "0", "0.000002", "0", "0")
        val features = (labels zip values).toMap[String, String]
        println(features)

        val data = linker.getInstance(features)
        println(data)

        (0 to data.size() - 1).foreach(i => {
            val unlabeled = data.instance(i)
            val clsLabel = svm.classifyInstance(unlabeled);
            println("Decision: %s // Instance: %s".format(clsLabel.toString, unlabeled))
        })

    }
}