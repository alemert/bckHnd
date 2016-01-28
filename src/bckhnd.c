/******************************************************************************/
/*                                                                            */
/*            H A N D L E   M Q   B A C K O U T   M E S S A G E S             */
/*                              */
/*                       B A C K O U T   H A N D L E R                        */
/*                              */
/*                              B C K H N D . C                               */
/*                                                                            */
/* -------------------------------------------------------------------------- */
/*                                                                            */
/*  functions:                                                  */
/*    - backoutHandler                                    */
/*    - moveMessages                            */
/*    - readMessage                              */
/*    - dumpMsg                      */
/*                                                        */
/******************************************************************************/

/******************************************************************************/
/*   I N C L U D E S                                                          */
/******************************************************************************/

// ---------------------------------------------------------
// system
// ---------------------------------------------------------
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>

// ---------------------------------------------------------
// MQ
// ---------------------------------------------------------
#include <cmqc.h>

// ---------------------------------------------------------
// own 
// ---------------------------------------------------------
#include <ctl.h>
#include <msgcat/lgstd.h>
#include <mqbase.h>

// ---------------------------------------------------------
// local
// ---------------------------------------------------------
#include <cmdln.h>
#include <bckhnd.h>
#include <msgmng.h>
#include <dirent.h>

/******************************************************************************/
/*   G L O B A L S                                                            */
/******************************************************************************/

/******************************************************************************/
/*   D E F I N E S                                                            */
/******************************************************************************/
#define INITIAL_MSG_SIZE 4096
#define DEFAULT_EXPIRY_TIME 3600
#define MIN_EXPIRY_TIME     600

/******************************************************************************/
/*   M A C R O S                                                              */
/******************************************************************************/

/******************************************************************************/
/*   P R O T O T Y P E S                                                      */
/******************************************************************************/
int moveMessages();

int readMessage( MQHCONN _hCon        ,  // connection handle   
                 MQHOBJ  _hGetQ       ,  // get queue handle
		 PMQMD   _pMd         ,  // message descriptor (set to default) 
                 PMQVOID *_pBuffer    ,  // message buffer
                 PMQLONG _pMaxMsgLng  ,  // maximal available message length
                 PMQLONG _pRealMsgLng);  // real message length

void dumpMsg( const char *path,
              MQMD md         , 
              char *buffer    , 
              int  realMsgLng);

/******************************************************************************/
/*                                                                            */
/*   F U N C T I O N S                                                        */
/*                                                                            */
/******************************************************************************/

/******************************************************************************/
/*  back out handler                          */
/******************************************************************************/
int backoutHandler()
{
  logFuncCall() ;               

  int sysRc = 0 ;

  MQHCONN hCon ;                         // connection handle   
  char qmgrName[MQ_Q_MGR_NAME_LENGTH+1]; // queue manager name 
                                         //
  MQHOBJ  hBoq ;                         // queue handle   
  MQOD    dBoq = {MQOD_DEFAULT};         // queue descriptor
                                         //
  MQHOBJ  hSrcq ;                        // queue handle   
  MQOD    dSrcq = {MQOD_DEFAULT};        // queue descriptor
                                         //
  MQHOBJ  hFwdq ;                        // queue handle   
  MQOD    dFwdq = {MQOD_DEFAULT};        // queue descriptor

  // -------------------------------------------------------
  // initialize message id list
  // -------------------------------------------------------
  sysRc = initMsgIdList();
  if( sysRc != 0 )
  {
    goto _door ;
  }

  // -------------------------------------------------------
  // connect to queue manager 
  // -------------------------------------------------------
  memset( qmgrName, ' ', MQ_Q_MGR_NAME_LENGTH );  
  qmgrName[MQ_Q_MGR_NAME_LENGTH] = '\0'  ;  // copy queue manager name
  memcpy( qmgrName                       ,  // from command line attributes
          getStrAttr( "qmgr" )           ,  // to vara
          strlen( getStrAttr( "qmgr" ) ));  //
                                            //
  sysRc =  mqConn( (char*) qmgrName,        // queue manager          
                           &hCon  );        // connection handle            
                                            //
  switch( sysRc )                           //
  {                                         //
    case MQRC_NONE :     break ;            // OK
    case MQRC_Q_MGR_NAME_ERROR :            // queue manager does not exists
    {                                       //
      logger(LMQM_UNKNOWN_QMGR,qmgrName);   //
      goto _door;                           //
    }                                       //
    default : goto _door;                   // error will be logged in mqConn
  }                                         //
                                            //
  // -------------------------------------------------------
  // open back out queue
  // -------------------------------------------------------
  memset( dBoq.ObjectName, ' ', MQ_Q_NAME_LENGTH );   
  dBoq.ObjectName[MQ_Q_NAME_LENGTH] = '\0' ;  //
  memcpy( dBoq.ObjectName   ,                 //
          getStrAttr("backout")           ,   //
          strlen(getStrAttr("backout")) );    //
                                              //
  sysRc=mqOpenObject( hCon                  , // connection handle
                      &dBoq                 , // queue descriptor
                      MQOO_INPUT_EXCLUSIVE  | // open for exclusive get
                      MQOO_FAIL_IF_QUIESCING, // fail if stopping queue manager
                      &hBoq );                // queue handle
                                              //
  switch( sysRc )                             //
  {                                           //
    case MQRC_NONE : break ;                  //
    default        : goto _door;              //
  }                                           //
                                              //
  // -------------------------------------------------------
  // open (original) source queue
  // -------------------------------------------------------
  memset( dSrcq.ObjectName, ' ', MQ_Q_NAME_LENGTH ); 
  dSrcq.ObjectName[MQ_Q_NAME_LENGTH] = '\0' ; //
  memcpy( dSrcq.ObjectName  ,                 //
          getStrAttr("source")          ,     //
          strlen( getStrAttr("source") ) );   //
                                              //
  sysRc=mqOpenObject( hCon                  , // connection handle
                      &dSrcq                , // queue descriptor
                      MQOO_OUTPUT           | // open object for put
                      MQOO_SET_ALL_CONTEXT  | // keep original date/time in MQMD
                      MQOO_FAIL_IF_QUIESCING, // fail if stopping queue manager
                      &hSrcq );               // queue handle
                                              //
  switch( sysRc )                             //
  {                                           //
    case MQRC_NONE : break ;                  //
    default        : goto _door;              //
  }                                           //
                                              //
  // -------------------------------------------------------
  // open forward queue
  // -------------------------------------------------------
  memset( dFwdq.ObjectName, ' ', MQ_Q_NAME_LENGTH ); 
  dFwdq.ObjectName[MQ_Q_NAME_LENGTH] = '\0' ; //
  memcpy( dFwdq.ObjectName  ,                 //
          getStrAttr("forward")          ,    //
          strlen( getStrAttr("forward") ) );  //
                                              //
  sysRc=mqOpenObject( hCon                  , // connection handle
                      &dFwdq                , // queue descriptor
                      MQOO_OUTPUT           | // open object for put
                      MQOO_SET_ALL_CONTEXT  | // keep original date/time in MQMD
                      MQOO_FAIL_IF_QUIESCING, // open options
                      &hFwdq );               // queue handle
                                              //
  switch( sysRc )                             //
  {                                           //
    case MQRC_NONE : break ;                  //
    default        : goto _door;              //
  }                                           //
                                              //
  // -------------------------------------------------------
  // move messages under sync point
  // -------------------------------------------------------
  while( 1 )
  {
    sysRc = moveMessages( hCon, hBoq, hSrcq, hFwdq );
    switch( sysRc ) 
    {
      case MQRC_NONE: continue ;
      case MQRC_NO_MSG_AVAILABLE:
      {
	sleep(1);
        continue;
      }
      default:
      {
	goto _door;
      }
    }
  }

  _door:

  logFuncExit( );
  return sysRc ;
}

/******************************************************************************/
/*   move messages                     */
/******************************************************************************/
int moveMessages( MQHCONN _hCon    ,     // connection handle   
                  MQHOBJ  _hGetQ   ,     // get queue handle
                  MQHOBJ  _hPutOrg ,     // put queue original handle 
                  MQHOBJ  _hPutFwd )     // get queue forward handle
{
  logFuncCall() ;               

  int sysRc = 0 ;
  MQLONG reason ;
  int    found  ;
  int    expiry = getIntAttr("expiry") ;

  MQMD  md  = {MQMD_DEFAULT} ;    // message descriptor (set to default)
  MQPMO pmo = {MQPMO_DEFAULT};    // put message option set to default
                                  //
                                  //
  static PMQVOID buffer = NULL;   //
                                  //
  static MQLONG maxMsgLng = INITIAL_MSG_SIZE ;
         MQLONG realMsgLng ;

  if( expiry == INT_MAX || expiry == INT_MIN )
  {
    expiry = DEFAULT_EXPIRY_TIME ;
  }
  if( expiry < MIN_EXPIRY_TIME )
  {
    expiry = MIN_EXPIRY_TIME ;
  }

  // -----------------------------------------------------
  // initialization of static vara with first call of this function
  // -----------------------------------------------------
  if( !buffer )                             //
  {                                         // message buffer, allocation 
    buffer = (PMQVOID) malloc( maxMsgLng ); // necessary only on first call
    if( !buffer )                           // of this function, since static
    {                                       //
      logger( LSTD_MEM_ALLOC_ERROR );       //
      sysRc = errno ;                       //
      goto _door;                           //
    }                                       //
  }                                         //
                                            //
  // -----------------------------------------------------
  // start transaction
  // -----------------------------------------------------
  reason = mqBegin( _hCon );                // begin transaction
  switch( reason )                          //
  {                                         //
    case MQRC_NONE :                        //
    case MQRC_NO_EXTERNAL_PARTICIPANTS :    // transactions without external 
    {                                       //  resource manager
      sysRc = MQRC_NONE;                    //
      break;                                //
    }                                       //
    default : goto _door;                   //
  }                                         //
                                            // 
  // -----------------------------------------------------
  // read the message
  // -----------------------------------------------------
  sysRc = readMessage( _hCon        ,       // connection handle   
                       _hGetQ       ,       // get queue handle
                       &md          ,       // message descriptor 
                       &buffer      ,       // message buffer
                       &maxMsgLng   ,       // message buffer length
                       &realMsgLng );       // real message length
                                            //
  switch( sysRc )                           //
  {                                         //
    case MQRC_NONE : break;                 //
    case MQRC_NO_MSG_AVAILABLE : goto _backout;
    default        : goto _door;            //
  }                                         //
                                            //
  found = chkMsgId( md.MsgId, expiry );     //
                                            //
  pmo.Options = MQPMO_FAIL_IF_QUIESCING +   //
                MQPMO_SET_ALL_CONTEXT   +   // for keeping old time / date
                MQPMO_SYNCPOINT         ;   //
                                            //
  switch( found )                //
  {                                         //
    // -----------------------------------------------------
    // put message back to the original queue
    // -----------------------------------------------------
    case MSG_ID_OVER_LIST_BUFFER :          //
    case MSG_ID_NOT_FOUND :                 //
    {                                       //
      sysRc = mqPut( _hCon       ,          // connection handle
                     _hPutOrg    ,          // original queue handle
                     &md         ,          // message descriptor
                     &pmo        ,          // Options controlling MQPUT
                     &buffer     ,          // message buffer
                     realMsgLng );          // message length (buffer length)
                              //
      switch( sysRc )                       //
      {                                     //
	case MQRC_NONE:               //
        {                          //
          sleep(1);              //
	  mqCommit( _hCon );        //
	  break;                    //
        }                            //
        default: goto _backout;        //
      }                                    //
      break;                        //
    }                                  //
                                        //
    // -----------------------------------------------------
    // put message to the forward queue  
    // -----------------------------------------------------
    case MSG_ID_FOUND :        //
    {                              //
      sysRc = mqPut( _hCon       ,          // connection handle
                     _hPutFwd    ,          // original queue handle
                     &md         ,          // message descriptor
                     &pmo        ,          // Options controlling MQPUT
                     &buffer     ,          // message buffer
                     realMsgLng );          // message length (buffer length)
                            //
      switch( sysRc )            //
      {                                     //
	case MQRC_NONE:             //
        {                                   //
	  if( getStrAttr("dump") )          //
	  {                //
            dumpMsg( getStrAttr("dump"),    //
                     md          ,      //
                     buffer      ,      //
                     realMsgLng );      //
	  }                      //
	  mqCommit( _hCon );      //
	  break;                //
        }                        //
        default: goto _backout;      //
      }                              //
      break;                      //
    }                                  //
  }                                  //
                                            //
  // -------------------------------------------------------
  // function exit point error or OK
  // -------------------------------------------------------
  _door:

  logFuncExit( );
  return sysRc ;

  // -------------------------------------------------------
  // function exit point for Error with rollback
  // -------------------------------------------------------
  _backout:

  reason = mqRollback( _hCon );         // roll back 
  if( reason != MQRC_NONE )             //
  {                                     //
    sysRc = reason;                     //
  }                                     //
 
  logFuncExit( );
  return sysRc ;
}

/******************************************************************************/
/*  read message                                                              */
/*                                                                            */
/*  description:                                                              */
/*    get a message from the queue, if available message buffer of maxMsgLng  */
/*    is not available, resize buffer                                         */
/*                                                                            */
/*   attributes:                                                              */
/*     1. _hCon       : connection handle                                     */
/*     2. _hGetQ      : get queue handle                                      */
/*     3. _pMd        : message descriptor, already set to MQMD_DEFAULT       */
/*     4. *_pBuffer   : message buffer, maxMsgLng allocated                   */
/*     5. _pMaxMsgLng : maximal available message length, can be increased    */
/*                      in readMessage by resizeMqMessageBuffer               */
/*     6. _pRealMsgLng: real message length, this information is needed by    */
/*                      later put call                                        */
/*                                                                            */
/******************************************************************************/
int readMessage( MQHCONN _hCon        ,  // connection handle   
                 MQHOBJ  _hGetQ       ,  // get queue handle
		 PMQMD   _pMd         ,  // message descriptor (set to default) 
                 PMQVOID *_pBuffer    ,  // message buffer
                 PMQLONG _pMaxMsgLng  ,  // maximal available message length
                 PMQLONG _pRealMsgLng )  // real message length
{
  logFuncCall() ;               

  int sysRc = 0 ;
  MQLONG reason ;

  MQGMO gmo = {MQGMO_DEFAULT};    // get message option set to default

  // -----------------------------------------------------
  // read the message
  // -----------------------------------------------------
  memcpy( _pMd->MsgId       ,               // flash message id, 
          MQMI_NONE         ,               // 
          sizeof(MQBYTE24) );               // MQBYTE24  
  _pMd->Version = MQMD_VERSION_2;           //
                                            //
  gmo.MatchOptions = MQMO_NONE        ;     // 
  gmo.Options      = MQGMO_CONVERT    +     //
                     MQGMO_SYNCPOINT  ,     //
                     MQGMO_WAIT       ;     //
  gmo.Version      = MQGMO_VERSION_3  ;     //
                                            //
  *_pRealMsgLng = *_pMaxMsgLng;             //
  reason = mqGet( _hCon       ,             // connection handle
                  _hGetQ      ,             // pointer to queue handle
                  *_pBuffer   ,             // message buffer
                  _pRealMsgLng,             // buffer length
                  _pMd        ,             // message descriptor
                  gmo         ,             // get message option
                  60000      );             // wait interval in milliseconds
                                            // (makes 1Minute)
  switch( reason )                          //
  {                                         //
    // ---------------------------------------------------
    // read was OK
    // ---------------------------------------------------
    case MQRC_NONE :                        //
    {                                       //
      break;                                //
    }                                       //
                                            //
    // ---------------------------------------------------
    // no message found -> OK
    // ---------------------------------------------------
    case MQRC_NO_MSG_AVAILABLE :            // no message available, necessary 
    {                                       //  for catching signals
      sysRc = reason ;                      //
      break;                                //
    }                                       //
                                            //
    // ---------------------------------------------------
    // an error occurred preventing later commit
    // ---------------------------------------------------
    case MQRC_BACKED_OUT:                   //
    {                                       //
      sysRc = reason;                       //
      goto _backout;                        //
    }                                       //
                                            //
    // ---------------------------------------------------
    // message buffer to small for the (physical) message 
    //   - rollback
    //   - resize buffer
    //   - re-read message
    // ---------------------------------------------------
    case MQRC_TRUNCATED_MSG_FAILED :        // 
    {                                       //
      reason = mqRollback( _hCon );         // roll back 
                                            //
      if( reason != MQRC_NONE )             //
      {                                     //
        sysRc = reason;                     //
        goto _door;                         //
      }                                     //
                                            //
      // -------------------------------------------------
      // resize buffer
      // ---------------------------------------------- // increase the buffer 
      *_pMaxMsgLng=((int)((*_pRealMsgLng)/1024)+1)*1024;// size to next full kB, 
      *_pBuffer = resizeMqMessageBuffer( *_pBuffer,     // f.e. 4500Byte to 5k
                                         _pMaxMsgLng ); // 
      if( !_pBuffer )                       //
      {                                     //
        sysRc = errno ;                     //
        if( sysRc == 0 ) sysRc = 1 ;        //
        goto _door;                         //
      }                                     //
                                            //
      // -------------------------------------------------
      // read the message with new buffer size
      // -------------------------------------------------
      *_pRealMsgLng = *_pMaxMsgLng;         //
                                            //
      reason=mqGet( _hCon       ,           // connection handle
                    _hGetQ      ,           // pointer to queue handle
                    *_pBuffer   ,           // message buffer
                    _pRealMsgLng,           // buffer length
                    _pMd        ,           // message descriptor
                    gmo         ,           // get message option
                    60000      );           // wait interval in milliseconds
                                            //
      switch( reason )                      //
      {                                     //
        case MQRC_NONE: break;              //
        default:                            //
        {                                   //
	  sysRc = reason;                   //
          goto _door;                       //
        }                                   //
      }                                     //
      break;                                //
    }                                       //
                                            //
    // ---------------------------------------------------
    // any other error
    // ---------------------------------------------------
    default:                                //
    {                                       //
      logMQCall(ERR,"MQGET",reason);        //
      sysRc = reason;                       //
      goto _backout ;                       //
    }                                       //
  }                                         //
                                            //
  // -------------------------------------------------------
  // exit point for OK and Error
  // -------------------------------------------------------
  _door:

  logFuncExit( );
  return sysRc ;

  // -------------------------------------------------------
  // exit point for Error with rollback
  // -------------------------------------------------------
  _backout:

  reason = mqRollback( _hCon );         // roll back 
  if( reason != MQRC_NONE )             //
  {                                     //
    sysRc = reason;                     //
  }                                     //
 
  logFuncExit( );
  return sysRc ;

}

/******************************************************************************/
/*   dump message                                                             */
/*                                                                            */
/*   description:                                                             */
/*     dump message including message description and message body in human   */
/*     readable format to the file                                            */
/*     the file should be written on the directory path and should have       */
/*     message id in the name                                                 */
/*                                                                        */
/*   attributes:                                                    */
/*     1. path                                                       */
/*     2. message descriptor                              */
/*     3. message buffer                                       */
/*     4. message length                               */
/*                            */
/*    return type:                            */
/*      void, no return code needed, if writing a file fails, no data will be */
/*      lost since real message is still on the queue            */
/*                                                                            */
/******************************************************************************/
void dumpMsg( const char *path,
              MQMD md         , 
              char *buffer    , 
              int  realMsgLng )
{
  logFuncCall() ;               

  FILE *fp ;

  MQBYTE   byte ;
  char msgId[sizeof(MQBYTE24)*2+1] ;
  msgId[sizeof(MQBYTE24)*2] = '\0';

  char fileName[MAXNAMLEN];

  int i;

  for(i=0;i<24;i++)
  {
    byte = md.MsgId[i] ;
    sprintf( &msgId[i*2],"%.2x",(int)md.MsgId[i]);
    printf("0x%2.2x %d %c\n",(int)byte, (int)byte, (char)byte);
  }

  snprintf(fileName,MAXNAMLEN,"%s/%s.%s.browse",path,getStrAttr("source"),msgId);

  printf("%s\n",fileName);
  fp = fopen(fileName,"w");
  if( !fp )
  {
    logger(LSTD_OPEN_FILE_FAILED,fileName);
    logger( LSTD_ERRNO_ERR, errno, strerror(errno) );
    goto _door;
  }
#if(0)
  setDumpItemStr(  F_MQCHAR4             ,
                  "Structure identifier" ,
                   md->StrucId           );           
 
  setDumpItemStr(  F_STR                 ,
                  "Structure version"    ,
                  (char*) mqmdVer2str(md->Version) );

  setDumpItemStr(  F_STR                ,
                  "Report msgs options" ,
                   (char*) mqReportOption2str(md->Report) );

  setDumpItemStr(  F_STR                ,
                   "Msg type"           ,
                   (char*) mqMsgType2str(md->MsgType) );           

  setDumpItemInt(  F_MQLONG             ,
                  "Msg lifetime"        ,
                   md->Expiry           );

  setDumpItemStr(  F_STR                ,
                  "Feedback code"       ,
                  (char*) mqFeedback2str( md->Feedback) );

  setDumpItemStr(  F_STR                      ,
                  "Msg data numeric encoding" ,
                  (char*) mqEncondig2str(md->Encoding) );

  setDumpItemStr(  F_STR           ,
                  "Msg data CCSID" ,
                  (char*) mqCCSID2str(md->CodedCharSetId) );
  
  setDumpItemStr(  F_MQCHAR8             ,
                  "Msg data Format name" ,
                   md->Format            );            
    
  setDumpItemStr(  F_STR                 ,
                  "Message priority"     ,
                  (char*) mqPriority2str(md->Priority) );          
      
  setDumpItemStr(  F_STR                 ,
                  "Message persistence"  ,
                  (char*)mqPersistence2str(md->Persistence) );
      
  setDumpItemByte(  F_MQBYTE24           ,
                   "Message identifier"  ,
                    md->MsgId            );
      
  setDumpItemByte(  F_MQBYTE24              ,
                   "Correlation identifier" ,
                    md->CorrelId            );
      
  setDumpItemInt(  F_MQLONG                 ,
                  "Backout counter"         ,
                   md->BackoutCount         );
    
  setDumpItemStr(  F_MQCHAR48               ,
                  "Name of reply queue"     ,
                   md->ReplyToQ             );
    
  setDumpItemStr(  F_MQCHAR48               ,
                  "Name of reply qmgr"      ,
                   md->ReplyToQMgr          );
     
  setDumpItemStr(  F_MQCHAR12               ,
                  "User identifier"         ,
                   md->UserIdentifier       );
    
  setDumpItemByte(  F_MQBYTE32              ,
                   "Accounting token"       ,
                    md->AccountingToken     );
    
  setDumpItemStr(  F_MQCHAR32                   ,
                  "Appl data relating identity" ,
                   md->ApplIdentityData         );
  
  setDumpItemStr(  F_STR          ,
                  "Appl Put Type" ,
                  (char*) mqPutApplType2str(md->PutApplType) );

  setDumpItemStr(  F_MQCHAR28         ,
                  "Putting appl name" ,
                   md->PutApplName    );       

  setDumpItemStr(  F_MQCHAR8          ,
                  "Put Date"          ,
                   md->PutDate        );
    
  setDumpItemStr(  F_MQCHAR8          ,
                  "Put time"          ,
                   md->PutTime        );

  setDumpItemStr(  F_MQCHAR4                     ,
                  "Appl data relating to origin" ,
                   md->ApplOriginData            );

  // -------------------------------------------------------
  // msg dscr version 2 or higher
  // -------------------------------------------------------
  if( md->Version < MQMD_VERSION_2 ) goto _door ;

  setDumpItemByte(  F_MQBYTE24        , 
                   "Group identifier" , 
                    md->GroupId       );

  setDumpItemInt(  F_MQLONG                       , 
                  "SeqNr of logical msg in group" , 
                   md->MsgSeqNumber               );

  setDumpItemInt(  F_MQLONG                         , 
                  "PhysMsg Offset from logic start" , 
                   md->Offset                       );

  setDumpItemStr(  F_STR          , 
                  "Message flags" , 
                   (char*) mqMsgFlag2str(md->MsgFlags) );

  setDumpItemInt(  F_MQLONG                    , 
                  "Length of original message" ,
                   md->OriginalLength          );
#endif
  _door:
  logFuncExit( );
}