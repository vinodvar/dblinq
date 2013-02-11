using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Transactions;
using NUnit.Framework;
using DbLinq.MySql;
using nwind;

namespace Test_NUnit.Context
{
    [TestFixture]
    public class MySqlDataContextTest : TestBase
    {
        private string ConnectionString(string autoEnlist = "")
        {
            const string dblinqProvider = "DbLinqProvider=MySql;";
            const string dblinqConnectionType = "DbLinqConnectionType=MySql.Data.MySqlClient.MySqlConnection, MySql.Data, Version=6.6.4.0, Culture=neutral, PublicKeyToken=c5687fc88969c44d";

            var connectionString = base.connStr;
            connectionString += autoEnlist;
            connectionString += dblinqProvider;
            connectionString += dblinqConnectionType;

            return connectionString;
        }

        [Test]
        public void Ctor_ConnectionString_AutoEnlistIsFalse_NotInHigherTransactionScope()
        {
            var ctx = new MySqlDataContext(ConnectionString("AutoEnlist=false;"));
           using (var scope = new TransactionScope())
           {
               Assert.IsFalse(ctx.InHigherTransactionScope());
           }
        }

        [Test]
        public void Ctor_ConnectionString_AutoEnlistIsTrue_InHigherTransactionScope()
        {
            var ctx = new MySqlDataContext(ConnectionString("AutoEnlist=true;"));
            using (var scope = new TransactionScope())
            {
                Assert.IsTrue(ctx.InHigherTransactionScope());
            }
        }

        [Test]
        public void Ctor_ConnectionString_NoAutoEnlist_InHigherTransactionScope()
        {
            var ctx = new MySqlDataContext(ConnectionString());
            using (var scope = new TransactionScope())
            {
                Assert.IsTrue(ctx.InHigherTransactionScope());
            }
        }

        [Test]
        public void Ctor_ConnectionString_NoAutoEnlist_NotInHigherTransactionScope()
        {
            var ctx = new MySqlDataContext(ConnectionString());
            Assert.IsFalse(ctx.InHigherTransactionScope());
        }

        [Test]
        public void Ctor_ConnectionString_AutoEnlistIsTrue_NotInHigherTransactionScope()
        {
            var ctx = new MySqlDataContext(ConnectionString("AutoEnlist=true;"));
            Assert.IsFalse(ctx.InHigherTransactionScope());
        }

        [Test]
        public void BulkInsert_InHigherTransactionScope_InsertProducts()
        {
            var ctx = new MySqlDataContext(ConnectionString());
            var initialCount = ctx.GetTable<Product>().Count();

            using (var scope = new TransactionScope())
            {
                ctx.GetTable<Product>().BulkInsert(new[]
                                       {
                                           TestBase.NewProduct("tmp_ProductA1"),
                                           TestBase.NewProduct("tmp_ProductB1"),
                                           TestBase.NewProduct("tmp_ProductC1"),
                                           TestBase.NewProduct("tmp_ProductD1")
                                       });
                ctx.SubmitChanges();
                scope.Complete();
            }
           
            //confirm that we indeed inserted four rows:
            var countAfterBulkInsert = ctx.GetTable<Product>().Count();
            Assert.IsTrue(countAfterBulkInsert == initialCount + 4);

            //clean up
            ctx.ExecuteCommand("DELETE FROM Products WHERE ProductName LIKE 'tmp_%'");
        }


        [Test]
        public void BulkInsert_InHigherTransactionScope_NotComplete_NoProductsInserted()
        {
            var ctx = new MySqlDataContext(ConnectionString());
            var initialCount = ctx.GetTable<Product>().Count();

            using (var scope = new TransactionScope())
            {
                ctx.GetTable<Product>().BulkInsert(new[]
                                       {
                                           NewProduct("tmp_ProductA1"),
                                           NewProduct("tmp_ProductB1"),
                                           NewProduct("tmp_ProductC1"),
                                           NewProduct("tmp_ProductD1")
                                       });
                ctx.SubmitChanges();
            }

            //confirm that we indeed inserted four rows:
            var countAfterBulkInsert = ctx.GetTable<Product>().Count();
            Assert.IsTrue(countAfterBulkInsert == initialCount );
        }

        [Test]
        public void Insert_InHigherTransactionScope_InsertProduct()
        {
            var ctx = new MySqlDataContext(ConnectionString());
            var initialCount = ctx.GetTable<Product>().Count();

            using (var scope = new TransactionScope())
            {
                ctx.GetTable<Product>().InsertOnSubmit(NewProduct("tmp_ProductA1"));
                ctx.SubmitChanges();
                scope.Complete();
            }

            //confirm that we indeed inserted four rows:
            var countAfterBulkInsert = ctx.GetTable<Product>().Count();
            Assert.IsTrue(countAfterBulkInsert == initialCount + 1);

            //clean up
            ctx.ExecuteCommand("DELETE FROM Products WHERE ProductName LIKE 'tmp_%'");
        }
        [Test]
        public void Insert_InHigherTransactionScope_NotComplete_NoProductsInserted()
        {
            var ctx = new MySqlDataContext(ConnectionString());
            var initialCount = ctx.GetTable<Product>().Count();

            using (var scope = new TransactionScope())
            {
                ctx.GetTable<Product>().InsertOnSubmit(NewProduct("tmp_ProductA1"));
                ctx.SubmitChanges();
            }

            //confirm that we indeed inserted four rows:
            var countAfterBulkInsert = ctx.GetTable<Product>().Count();
            Assert.IsTrue(countAfterBulkInsert == initialCount);
        }
    }
}
