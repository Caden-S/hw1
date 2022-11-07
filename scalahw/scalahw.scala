case class Neumaier(sum: Double, c: Double)

object HW {

   def q1_countsorted(x: Int, y: Int, z:Int) : Int = {
      //the types of the input parameters have been declared.
      //you must do the same for the output type (see scala slides)
      //do not use return statements.
      val a = if (x < y) { 1 } else { 0 }
      val b = if (y < z) { 1 } else { 0 }
      val c = if (x < z) { 1 } else { 0 }
      a + b + c
   }

   def q2_interpolation(name: String, age: Int) : String = {
      //the types of the input parameters have been declared.
      //you must do the same for the output type (see scala slides)
      //do not use return statements.
      if (age < 21) { s"howdy, $name".toLowerCase() } else { s"hello, $name".toLowerCase() }
   }

   def q3_polynomial(arr: Seq[Double]) : Double = {
      //the types of the input parameters have been declared.
      //you must do the same for the output type (see scala slides)
      //do not use return statements.
      val init = (0.0, 1.0)
      arr.foldLeft(init)((state, x) => (state._1 + (x * state._2), state._2 + 1))._1
   }

   def q4_application(x: Int, y: Int, z: Int)(f: (Int, Int) => Int) : Int = {
      //the types of the input parameters have been declared.
      //you must do the same for the output type (see scala slides)
      //do not use return statements.
      f(f(x,y), z)
   }
   
   def q5_stringy(start: Int, n: Int) : Vector[String] = {
      Vector.tabulate(n){x => (x + start).toString}
   }

   def q6_modab(a: Int, b: Int, c: Vector[Int]) : Vector[Int] = {
      val rules = { x: Int => ((x >= a) && (x % b > 0)) }
      c.filter(rules(_) == true)
   }
   def q7_count(arr: Vector[Int])(f: Int => Boolean) : Int = {
      if (arr.isEmpty == false) {
         if (f(arr.head) == true) {
            1 + q7_count(arr.tail)(f)
         } else {
            q7_count(arr.tail)(f)
         }
      } else {
        0
      }
   }

   @annotation.tailrec
   def q8_count_tail(arr: Vector[Int], count: Int)(f: Int => Boolean) : Int = {
      if (arr.isEmpty == false) {
         if (f(arr.head) == true) {
            q8_count_tail(arr.tail, count + 1)(f)
         } else {
            q8_count_tail(arr.tail, count)(f)
         }
      } else {
         count
      }
   }
   
   def q9_neumaier(in: Seq[Double]) = {
      val new_c = { (old_c: Double, input: Double, s: Double) => if (s.abs >= input.abs) { 
                                                               (s - (s + input)) + input 
                                                            } else { 
                                                               (input - (s + input)) + s
                                                            } 
              }
      val init = Neumaier(0.0, 0.0)
      val ans = in.foldLeft(init)((state, x) => Neumaier(state.sum + x, new_c(state.c, x, state.sum) ) )
      ans.sum + ans.c
   }
}
