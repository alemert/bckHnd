/******************************************************************************/
/* change title on for new project                                            */
/******************************************************************************/

/******************************************************************************/
/*   I N C L U D E S                                                          */
/******************************************************************************/
// ---------------------------------------------------------
// system
// ---------------------------------------------------------

// ---------------------------------------------------------
// own 
// ---------------------------------------------------------

/******************************************************************************/
/*   D E F I N E S                                                            */
/******************************************************************************/
#define MSG_ID_NOT_FOUND 0
#define MSG_ID_FOUND     1
#define MSG_ID_OVER_LIST_BUFFER 2 

/******************************************************************************/
/*   T Y P E S                                                                */
/******************************************************************************/

/******************************************************************************/
/*   S T R U C T S                                                            */
/******************************************************************************/

/******************************************************************************/
/*   G L O B A L E S                                                          */
/******************************************************************************/

/******************************************************************************/
/*   M A C R O S                                                              */
/******************************************************************************/

/******************************************************************************/
/*   P R O T O T Y P E S                                                      */
/******************************************************************************/
int initMsgIdList();
int chkMsgId( MQBYTE24 _msgId, int _expiry );
PMQBYTE24 findOldestMessage();