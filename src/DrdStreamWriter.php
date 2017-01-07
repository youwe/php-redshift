<?php
/**
 * Created by PhpStorm.
 * User: minhao
 * Date: 2016-08-19
 * Time: 15:18
 */

namespace Oasis\Mlib\Redshift;

class DrdStreamWriter
{
    const REPLACE_FROM = [
        "/\\\\/",
        "/\n/",
        "/\r/",
        "/\\|/",
    ];
    const REPLACE_TO   = [
        "\\\\\\\\",
        "\\\n",
        "\\\r",
        "\\|",
    ];
    
    protected $stream;
    protected $fieldNames;
    
    public function __construct($stream, $fieldNames)
    {
        if (!is_resource($stream)) {
            throw new \InvalidArgumentException("First argument must be an open stream");
        }
        
        $this->stream     = $stream;
        $this->fieldNames = $fieldNames;
    }
    
    public function writeRecord($obj)
    {
        $line      = '';
        $not_first = false;
        foreach ($this->fieldNames as $k) {
            if ($not_first) {
                $line .= "|";
            }
            else {
                $not_first = true;
            }
            
            if (is_array($obj)) {
                $v = isset($obj[$k]) ? $obj[$k] : '';
            }
            else {
                $v = property_exists($obj, $k) ? $obj->$k : '';
            }
            
            $v = preg_replace(self::REPLACE_FROM, self::REPLACE_TO, $v);
            $line .= $v;
        }
        
        fwrite($this->stream, $line);
        fwrite($this->stream, "\n");
    }
    
    public function close()
    {
        fclose($this->stream);
    }
}
