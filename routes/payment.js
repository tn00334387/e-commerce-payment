const express = require('express');
const router = express.Router();
const PaymentModule = require('../modules/payment');

// 定義註冊和登錄路由
router.post('/payment', PaymentModule.ProcessPayment);
router.post('/paymentQueue', PaymentModule.ProcessPaymentQ);
router.get('/payment/:userId', PaymentModule.GetPayment);

module.exports = router;