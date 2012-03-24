<?php
require_once(__DIR__.'/../AutopackBase.php');

use phpcassa\ColumnFamily;

abstract class SuperBase extends AutopackBase {

    protected function make_super_group($cf, $cols) {
        if ($this->SERIALIZED) {
            $dict = array();
            $serialized_cols = array();
            for ($i = 0; $i < count($cols); $i++) {
                $name = $cols[$i];
                $name = serialize($name);
                $dict[$name] = array('bytes' => self::$VALS[$i]);
                $serialized_cols[] = $name;
            }
        } else {
            $dict = array($cols[0] => array('bytes' => self::$VALS[0]),
                          $cols[1] => array('bytes' => self::$VALS[1]),
                          $cols[2] => array('bytes' => self::$VALS[2]));
            $serialized_cols = $cols;
        }
        return array(
            'cf' => $cf,
            'cols' => $cols,
            'serialized_cols' => $serialized_cols,
            'dict' => $dict
        );
    }

    public function test_super_column_families() {
        $type_groups = $this->make_type_groups();

        foreach($type_groups as $group) {
            $cf = $group['cf'];
            $dict = $group['dict'];
            $cols = $group['cols'];
            $serialized_cols = $group['serialized_cols'];

            $cf->insert(self::$KEYS[0], $dict);
            $this->assertEquals($dict, $cf->get(self::$KEYS[0]));

            # Check each column individually
            foreach(range(0,2) as $i)
                $this->assertEquals(
                    array($serialized_cols[$i] => array('bytes' => self::$VALS[$i])),
                    $cf->get(self::$KEYS[0], $columns=array($cols[$i])));

            # Check with list of all columns
            $this->assertEquals($dict, $cf->get(self::$KEYS[0], $columns=$cols));

            # Same thing but with start and end
            $this->assertEquals($dict,
                $cf->get(self::$KEYS[0], $columns=null,
                                         $column_start=$cols[0],
                                         $column_finish=$cols[2]));

            # Start and end are the same
            $this->assertEquals(
                array($serialized_cols[0] => array('bytes' => self::$VALS[0])),
                $cf->get(self::$KEYS[0], $columns=null,
                                         $column_start=$cols[0],
                                         $column_finish=$cols[0]));

            ### remove() tests ###

            $cf->remove(self::$KEYS[0], $columns=array($cols[0]));
            $this->assertEquals(2, $cf->get_count(self::$KEYS[0]));

            $cf->remove(self::$KEYS[0], $columns=array($cols[1], $cols[2]));
            $this->assertEquals(0, $cf->get_count(self::$KEYS[0]));

            # Insert more than one row
            $cf->insert(self::$KEYS[0], $dict);
            $cf->insert(self::$KEYS[1], $dict);
            $cf->insert(self::$KEYS[2], $dict);


            ### multiget() tests ###

            $result = $cf->multiget(self::$KEYS);
            foreach(range(0,2) as $i)
                $this->assertEquals($dict, $result[self::$KEYS[0]]);

            $result = $cf->multiget(array(self::$KEYS[2]));
            $this->assertEquals($result[self::$KEYS[2]], $dict);

            # Check each column individually
            foreach(range(0,2) as $i) {
                $result = $cf->multiget(self::$KEYS, $columns=array($cols[$i]));
                foreach(range(0,2) as $j)
                    $this->assertEquals(
                        array($serialized_cols[$i] => array('bytes' => self::$VALS[$i])),
                        $result[self::$KEYS[$j]]);

            }

            # Check that if we list all columns, we get the full dict
            $result = $cf->multiget(self::$KEYS, $columns=$cols);
            foreach(range(0,2) as $i)
                $this->assertEquals($dict, $result[self::$KEYS[$j]]);

            # The same thing with a start and end instead
            $result = $cf->multiget(self::$KEYS, $columns=null,
                                    $column_start=$cols[0],
                                    $column_finish=$cols[2]);
            foreach(range(0,2) as $i)
                $this->assertEquals($dict, $result[self::$KEYS[$j]]);

            # A start and end that are the same
            $result = $cf->multiget(self::$KEYS, $columns=null,
                                    $column_start=$cols[0],
                                    $column_finish=$cols[0]);
            foreach(range(0,2) as $i)
                $this->assertEquals(
                    array($serialized_cols[0] => array('bytes' => self::$VALS[0])),
                    $result[self::$KEYS[$j]]);


            ### get_range() tests ###

            $result = $cf->get_range($key_start=self::$KEYS[0]);
            foreach($result as $subres)
                $this->assertEquals($subres, $dict);

            $result = $cf->get_range($key_start=self::$KEYS[0], $key_finish='',
                                     $key_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                     $columns=null,
                                     $column_start=$cols[0],
                                     $column_finish=$cols[2]);
            foreach($result as $subres)
                $this->assertEquals($dict, $subres);

            $result = $cf->get_range($key_start=self::$KEYS[0], $key_finish='',
                                     $key_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                     $columns=$cols);
            foreach($result as $subres)
                $this->assertEquals($dict, $subres);
        }
    }
}
