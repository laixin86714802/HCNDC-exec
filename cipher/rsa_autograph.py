# !/usr/bin/env python
# -*- coding: utf-8 -*-

from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5
from Crypto.Hash import SHA512


def rsa_sign(plaintext, private_file, hash_algorithm=SHA512):
    """RSA 数字签名"""
    private_key = open(private_file, 'rb').read()
    signer = PKCS1_v1_5.new(RSA.importKey(private_key))
    # hash算法必须要pycrypto库里的hash算法，不能直接用系统hashlib库，pycrypto是封装的hashlib
    hash_value = hash_algorithm.new(plaintext)
    return signer.sign(hash_value)


def rsa_verify(sign, plaintext, public_file, hash_algorithm=SHA512):
    """校验RSA 数字签名"""
    public_key = open(public_file, 'rb').read()
    hash_value = hash_algorithm.new(plaintext)
    verifier = PKCS1_v1_5.new(RSA.importKey(public_key))
    return verifier.verify(hash_value, sign)


if __name__ == '__main__':
    message = 'RSA数字签名演示'
    # 生成签名
    signature = rsa_sign(message.encode(encoding='utf-8'), './private_key.pem')
    print(signature)
    # 验证签名
    result = rsa_verify(signature, message.encode('utf-8'), './public_key.pem')
    print(result)
