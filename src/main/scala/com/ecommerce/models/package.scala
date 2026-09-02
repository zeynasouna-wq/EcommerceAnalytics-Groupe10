package com.ecommerce

package object models {

  case class Transaction(
    transaction_id: String,
    user_id: String,
    product_id: String,
    merchant_id: String,
    amount: Double,
    timestamp: String,
    location: String,
    payment_method: String,
    category: String
  )

  case class User(
    user_id: String,
    age: Int,
    annual_income: Double,
    city: String,
    customer_segment: String,
    preferred_categories: Seq[String],
    registration_date: String
  )

  case class Product(
    product_id: String,
    name: String,
    category: String,
    price: Double,
    merchant_id: String,
    rating: Double,
    stock: Int
  )

  case class Merchant(
    merchant_id: String,
    name: String,
    category: String,
    region: String,
    commission_rate: Double,
    establishment_date: String
  )
}