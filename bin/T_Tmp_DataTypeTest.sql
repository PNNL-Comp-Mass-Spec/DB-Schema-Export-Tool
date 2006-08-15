/****** Object:  Table [dbo].[T_Tmp_DataTypeTest]    Script Date: 08/14/2006 18:10:00 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[T_Tmp_DataTypeTest](
	[UniqueID] [int] IDENTITY(1,1) NOT NULL,
	[BigIntCol] [bigint] NULL,
	[BinaryCol] [binary](50) NULL,
	[BitCol] [bit] NULL,
	[CharCol] [char](10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[DateTimeCol] [datetime] NULL,
	[DecimalCol] [decimal](18, 5) NULL,
	[FloatCol] [float] NULL,
	[ImageCol] [image] NULL,
	[IntCol] [int] NULL,
	[MoneyCol] [money] NULL,
	[NCharCol] [nchar](10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[NTextCol] [ntext] COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[NumericCol] [numeric](18, 5) NULL,
	[NVarCharCol] [nvarchar](50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[RealCol] [real] NULL,
	[SmallDateTimeCol] [smalldatetime] NULL,
	[SmallIntCol] [smallint] NULL,
	[SmallMoneyCol] [smallmoney] NULL,
	[SqlVariantCol] [sql_variant] NULL,
	[TextCol] [text] COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[TimeStampCol] [timestamp] NULL,
	[TinyIntCol] [tinyint] NULL,
	[UniqueIDCol] [uniqueidentifier] NULL,
	[VarBinaryCol] [varbinary](50) NULL,
	[VarCharCol] [varchar](50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]

GO
