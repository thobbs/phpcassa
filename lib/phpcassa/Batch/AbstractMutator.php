<?php

namespace phpcassa\Batch;

/**
 * Common methods shared by CfMutator and Mutator classes
 */
abstract class AbstractMutator
{
   abstract public function send($consistency_level=null);
}
