<?php
/**
 * Created by PhpStorm.
 * User: minhao
 * Date: 2017-06-30
 * Time: 18:22
 */

namespace Oasis\Mlib\Redshift;

interface CredentialProviderInterface
{
    public function getCredentialString();
}
