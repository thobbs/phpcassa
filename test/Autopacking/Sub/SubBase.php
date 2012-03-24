<?php
require_once(__DIR__.'/../AutopackBase.php');

use phpcassa\ColumnFamily;

abstract class SubBase extends AutopackBase {

    protected function make_sub_group($cf, $cols) {
        if ($this->SERIALIZED) {
            $subs = array();
            $serialized_cols = array();
            for ($i = 0; $i < count($cols); $i++) {
                $name = $cols[$i];
                $name = serialize($name);
                $subs[$name] = self::$VALS[$i];
            }
            $dict = array(222222222222 => $subs);
        } else {
            $dict = array(222222222222 => array($cols[0] => self::$VALS[0],
                                                $cols[1] => self::$VALS[1],
                                                $cols[2] => self::$VALS[2]));
        }
        return array(
            'cf' => $cf,
            'cols' => $cols,
            'dict' => $dict
        );
    }

    public function test_super_column_family_subs() {
        $LONG = 222222222222;
        $type_groups = $this->make_type_groups();

        foreach($type_groups as $group) {
            $cf = $group['cf'];
            $dict = $group['dict'];

            $cf->insert(self::$KEYS[0], $dict);
            $this->assertEquals($dict, $cf->get(self::$KEYS[0]));
            $this->assertEquals($dict, $cf->get(self::$KEYS[0], $columns=array($LONG)));

            # A start and end that are the same
            $this->assertEquals($dict,
                $cf->get(self::$KEYS[0], $columns=null,
                                         $column_start=$LONG,
                                         $column_finish=$LONG));

            $this->assertEquals(1, $cf->get_count(self::$KEYS[0]));

            ### remove() tests ###

            $cf->remove(self::$KEYS[0], $columns=null, $super_column=$LONG);
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
            $this->assertEquals($dict, $result[self::$KEYS[2]]);

            $result = $cf->multiget(self::$KEYS, $columns=array($LONG));
            foreach(range(0,2) as $i)
                $this->assertEquals($dict, $result[self::$KEYS[$i]]);

            $result = $cf->multiget(self::$KEYS,
                                    $columns=null,
                                    $column_start='',
                                    $column_finish='',
                                    $column_reverse=False,
                                    $count=ColumnFamily::DEFAULT_COLUMN_COUNT,
                                    $supercolumn=$LONG);
            foreach(range(0,2) as $i)
                $this->assertEquals($dict[$LONG], $result[self::$KEYS[$i]]);

            $result = $cf->multiget(self::$KEYS,
                                    $columns=null,
                                    $column_start=$LONG,
                                    $column_finish=$LONG);
            foreach(range(0,2) as $i)
                $this->assertEquals($dict, $result[self::$KEYS[$i]]);

            ### get_range() tests ###

            $result = $cf->get_range($key_start=self::$KEYS[0]);
            foreach($result as $subres) {
                $this->assertEquals($dict, $subres);
            }

            $result = $cf->get_range($key_start=self::$KEYS[0], $key_finish='',
                                     $row_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                     $columns=null,
                                     $column_start=$LONG,
                                     $column_finish=$LONG);
            foreach($result as $subres)
                $this->assertEquals($dict, $subres);

            $result = $cf->get_range($key_start=self::$KEYS[0],
                                     $key_finish='',
                                     $row_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                     $columns=array($LONG));
            foreach($result as $subres)
                $this->assertEquals($dict, $subres);

            $result = $cf->get_range($key_start=self::$KEYS[0],
                                     $key_finish='',
                                     $row_count=ColumnFamily::DEFAULT_ROW_COUNT,
                                     $columns=null,
                                     $column_start='',
                                     $column_finish='',
                                     $column_revered=False,
                                     $column_count=ColumnFamily::DEFAULT_COLUMN_COUNT,
                                     $super_column=$LONG);
            foreach($result as $subres)
                $this->assertEquals($dict[$LONG], $subres);
        }
    }
}
