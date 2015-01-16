package org.dbpedia.spotlight.db

import org.dbpedia.spotlight.db.memory.{MemoryTokenTypeStore, MemoryStore, MemorySurfaceFormStore}
import java.io.{File, FileInputStream}
import org.dbpedia.spotlight.model.{Text, Token, SurfaceForm}
import java.util.Locale
import org.dbpedia.spotlight.db.model.{TextTokenizer, Stemmer}
import org.dbpedia.spotlight.db.stem.SnowballStemmer
import org.dbpedia.spotlight.db.tokenize.LanguageIndependentTokenizer

/**
 * Copyright 2014 Idio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @author David Przybilla david.przybilla@idioplatform.com
 **/



class StemStoreGenerator {






}


object StemStoreGenerator{




  val locale = new Locale("en", "US")
  val stemmer: Stemmer = new SnowballStemmer("EnglishStemmer")


  def main(args:Array[String]){

    println("Reading stores")
    generateStemStore(args(0))
  }



  def exportStore(pathToOutputSfStore: String){


  }

  private def regenerateLowercaseMap(sfStore: MemorySurfaceFormStore, tokenStore:MemoryTokenTypeStore ){



    //create the tokenizer
    val tokenizer: TextTokenizer = new LanguageIndependentTokenizer(Set[String](), stemmer, locale, tokenStore)


    // cleaning the lowercase store
    sfStore.lowercaseMap.clear()


    // iterate the sfStore

    sfStore.iterateSurfaceForms.filter(_.annotationProbability >= 0.05).grouped(100000).toList.par.flatMap(_.map{
      sf: SurfaceForm =>
      //Tokenize all SFs first
        ( sf, tokenizer.tokenize(new Text(sf.name)))
    }).seq.foreach{
      case (sf: SurfaceForm, tokens: Seq[Token]) if tokens.size > 0 => {


        val filteredTokens = tokens.filter(_.token != SurfaceFormCleaner.FAKE_TOKEN_NAME )

        val stemmedSurfaceForm = filteredTokens.mkString(" ")
        if (sfStore.lowercaseMap.containsKey(stemmedSurfaceForm)){

          val currentCandidates = sfStore.lowercaseMap.get(stemmedSurfaceForm)
          sfStore.lowercaseMap.put(stemmedSurfaceForm, currentCandidates:+sf.id)


        }else{

          sfStore.lowercaseMap.put(stemmedSurfaceForm, Array[Int](sf.id))
        }

        val otherCandidatesArray = sfStore.lowercaseMap.get(stemmedSurfaceForm)
        if (otherCandidatesArray.length >0){

          val otherCandidates= otherCandidatesArray.map{
            candidateId:Int =>
              sfStore.stringForID(candidateId)

          }.mkString("\n\t\t")
          println("added.."+ sf.name )
          println("\t stem:"+ stemmedSurfaceForm)
          println("\tsame set as"+otherCandidates)
        }





      }
    }

  }

 def generateStemStore(pathtoFolder: String){



   val quantizedStore = MemoryStore.loadQuantizedCountStore(new FileInputStream(new File(pathtoFolder, "quantized_counts.mem")))

   // read the previous store
   val sfMemFile = new FileInputStream(new File(pathtoFolder, "sf.mem"))
   var sfStore: MemorySurfaceFormStore = MemoryStore.loadSurfaceFormStore(sfMemFile, quantizedStore)

   //load the token store
   val tokenMemFile = new FileInputStream(new File(pathtoFolder, "tokens.mem"))
   var tokenStore: MemoryTokenTypeStore = MemoryStore.loadTokenTypeStore(tokenMemFile)


   // regenerate the lowercase Store
   regenerateLowercaseMap(sfStore, tokenStore)

   // serialize
   try {

       MemoryStore.dump(sfStore, new File(pathtoFolder, "sf.mem"))
   } catch {
     case ex: Exception => {
       println(ex.getMessage)
     }
   }

   // happy town

 }

}
