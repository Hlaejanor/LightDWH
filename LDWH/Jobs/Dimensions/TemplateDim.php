<?php

/* 
 * TEMPLATE DIMENSION
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

class TemplateDim extends Dimension{
    public $ldwhConnection;
    
    function __construct(){    
  
        return true;
    }
    
    
   function createDimensionTable(){
       
       
   }
   /*
    * This wil return the coordinate id if the row exists in the dimension
    * Change the dimension variables 
    */
   
   function getCoord($variable1, $variable2){
       $sql = "SELECT DIMID from TEMPLATEDIM 
           WHERE  DIMVAR1 = ? 
           AND  DIMVAR2 = ? ";
       
       $stmt = $this->ldwhConnection->prepare($sql);
       if($stmt === FALSE){ print 'Error in sql:'.$this->ldwhConnection->error;}
       
       $stmt->bind_param(
               'ss', 
            $variable1, $variable2);
       $stmt->execute();
       $stmt->store_result();
       $stmt->bind_result($dimId);
       if($stmt->num_rows == 0){
             return $this->insertCoord($variable1, $variable2);
       }
       return $dimId;
   }
   
   
   /*
    *  Insert a coordiante into the dimension table and return the id
    */
   
   function insertCoord($variable1, $variable2){
         $sql2 = 'INSERT INTO dimTime ('
                 . 'DIMVAR1, '
                 . 'DIMVAR2, ' 
                 . 'values (?,?)'; // ! Add = for each variable
         $stmt = $this->ldwhConnection->prepare($sql);
         if($stmt === FALSE){ print 'Error in sql:'.$this->ldwhConnection->error;} 
          $stmt->bind_param
                  ('ssss', // ! Add one s for each variable
                       $variable1, $variable2
                  );
        $stmt->execute();
        $stmt->store_result();
        if($stmt->insert_id > 0){
           return $stmt->insert_id;
        }
        else{
            print "Error inserting id";
            return false;
        }
   }
}

?>