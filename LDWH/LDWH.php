<?php

/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */


class LDWH{
    public $ldwhConnection;
    
    function __construct($source){    
        $this->ldwhConnection =  new mysqli(LDWHServer, LDWHuser, LDWHPass, LDWHDb);
        $this->source = $source;
        return true;
    }
    function begin(){
        // TImes start
        
    }
    function end(){
        // Time end
        
    }
    function logMarker(){
        
        
    }
   	
        
}

?>