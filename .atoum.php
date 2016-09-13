<?php

$runner->addTestsFromDirectory( __DIR__ . '/Tests');

$script->excludeDirectoriesFromCoverage([
    __DIR__.'/vendor'
]);
