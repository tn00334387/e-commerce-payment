const Payment = require('../models/payment');
const Kafka = require('../libs/kafka')
const axios = require('axios');
const KafkaService = new Kafka(process.env.KAFKA_HOST_URI);

async function initProducer() {
  try {
    await KafkaService.initProducer();
    console.log('Kafka Producer initialized');
  } catch (err) {
    console.error('Error initializing Kafka producer:', err);
  }
}
initProducer();

const PaymentModule = {

    ProcessPayment: async ( req, res ) => {

        const { orderId, userId, amount, paymentMethod } = req.body;

        try {

            const payment = new Payment({
                orderId,
                userId,
                amount,
                paymentMethod,
                status: 'Pending',
            });
    
            await payment.save();
            const isPaymentSuccessful = await pay(userId)
            payment.status = isPaymentSuccessful ? 'Completed' : 'Failed'
            await payment.save();
    
            console.log(`user-${userId} process payment ${isPaymentSuccessful}`)
            if (isPaymentSuccessful) {
                await axios.put(`${process.env.ORDER_HOST_URI}/api/order/orders/${orderId}`, { status: 'Paid' });
                res.json({ 
                    message: 'Payment successful', 
                    payment 
                });
            } else {
                res.status(422).json({ 
                    message: 'Payment failed', 
                    payment 
                });
            }

        } catch (error) {
            console.log(`Payment - ProcessPayment : `, error)
            res.status(500).json({ 
                status: `Failed`,
                message: 'ProcessPayment failed' 
            });
        }
    },

    ProcessPaymentQ: async ( req, res ) => {

        const { orderId, userId, amount, paymentMethod } = req.body;

        try {

            const payment = new Payment({
                orderId,
                userId,
                amount,
                paymentMethod,
                status: 'Pending',
            });
    
            await payment.save();


            const isPaymentSuccessful = await pay(userId)
            payment.status = isPaymentSuccessful ? 'Completed' : 'Failed'
            await payment.save();
    
            console.log(`user-${userId} process payment ${isPaymentSuccessful}`)
            if (isPaymentSuccessful) {

                const message = JSON.stringify({ orderId, status: 'Paid' });
                await KafkaService.sendMessage('ec-payment', message);

                res.json({ 
                    message: 'Payment successful', 
                    payment 
                });
            } else {
                res.status(422).json({ 
                    message: 'Payment failed', 
                    payment 
                });
            }

        } catch (error) {
            console.log(`Payment - ProcessPayment : `, error)
            res.status(500).json({ 
                status: `Failed`,
                message: 'ProcessPayment failed' 
            });
        }
    },

    GetPayment: async ( req, res ) => {
        const { id: paymentId } = req.params
        try {

            const payment = await Payment.findById(paymentId);
            if (!payment) {
                console.log(`GetPayment : payment-${paymentId} is unexist`)
                res.status(404).json({ 
                    status: `NOT_FOUND`,
                    message: 'Payment not found' 
                })
                return 
            }
    
            res.json(payment);

        } catch (error) {
            console.log(`Payment - GetPayment : `, error)
            res.status(500).json({ 
                status: `Failed`,
                message: 'GetPayment failed' 
            });
        }
    },

}

module.exports = PaymentModule;

function pay( userId ) {
    const payTime = 1000 + Math.random() * 3000
    const status = Math.random() > 0.1; // 模拟80%成功率
    return new Promise(res => setTimeout( () => {
        console.log(`${userId} pay : ${status}`)
        res(status)
    }, payTime));
}